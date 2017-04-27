package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models._
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._
/**
  * Calculate cosine similarity for contexts of every term and link and
  * writes it with the other features as Feature Entries to the Cassandra.
  */
object CosineContextComparator extends SparkJob {
	appName = "Cosine Context Comparator"
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"
	val inputDocumentFrequenciesTablename = "wikipediadocfreq"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries with their term frequencies and loads the document frequencies from the
	  * Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val tfArticles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		val documentFrequencies = sc.cassandraTable[DocumentFrequency](keyspace, inputDocumentFrequenciesTablename)
		List(tfArticles).toAnyRDD() ++ List(documentFrequencies).toAnyRDD()
	}
	/**
	  * Saves the Feature Entries to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {

	}
	// $COVERAGE-ON$

	/**
	  * Transforms the term frequencies in a bag and an identifier for the term frequencies in a joinable format
	  * so that a join can be performed on the terms to calculate the tfIdf.
	  *
	  * @param identifier identifier for the terms contained in the bag
	  * @param tfBag      Bag containing the terms with their frequencies
	  * @tparam T type of the identifier (e.g. String for the article term frequencies and Link for the context
	  *           term frequencies.
	  * @return list of tuples with the structure (term, (identifier, term frequency))
	  */
	def joinableTermFrequencies[T](identifier: T, tfBag: Bag[String, Int]): List[(String, (T, Int))] = {
		tfBag.getCounts()
			.map { case (term, frequency) =>
				(term, (identifier, frequency))
			}.toList
	}

	/**
	  * Calculates the inverse document frequency (idf) for a given Document Frequency and the number of documents used
	  * to calculate it.
	  *
	  * @param df      document frequency to use
	  * @param numDocs number of documents used to compute the document frequency
	  * @return tuple of term and calculated idf
	  */
	def calculateIdf(df: DocumentFrequency, numDocs: Long): (String, Double) = {
		val idf = calculateIdf(df.count, numDocs)
		(df.word, idf)
	}

	/**
	  * Calculates the inverse document frequency (idf) using the logarithm base 10 for a given Document Frequency and
	  * the number of documents used to calculate it.
	  *
	  * @param df      document frequency to use
	  * @param numDocs number of documents used to compute the document frequency
	  * @return calculated inverse document frequency
	  */
	def calculateIdf(df: Int, numDocs: Long): Double = {
		assert(df > 0, s"Document frequency $df is not greater than 0.")
		Math.log10(numDocs.toDouble / df.toDouble)
	}

	/**
	  * Calculates the tfIdf of every term for every Wikipedia article.
	  *
	  * @param articles                   RDD of parsed Wikipedia articles with known term frequencies
	  * @param documentFrequencies        RDD of document frequencies
	  * @param numDocuments               number of documents that was used to calculate the document frequencies
	  * @param documentFrequencyThreshold lower threshold for document frequencies
	  * @return RDD of tuples containing the page name and the tfIdfs of every term on that page
	  */
	def calculateArticleTfidf(
		articles: RDD[ParsedWikipediaEntry],
		documentFrequencies: RDD[DocumentFrequency],
		numDocuments: Long,
		documentFrequencyThreshold: Int
	): RDD[(String, Map[String, Double])] = {
		val termFrequencies = articles
			.map(entry => (entry.title, Bag(entry.context)))
			.flatMap(t => joinableTermFrequencies(t._1, t._2))

		calculateTfidf(
			termFrequencies,
			documentFrequencies,
			numDocuments,
			documentFrequencyThreshold)
			.reduceByKey(_ ++ _)
	}

	/**
	  * Calculates the tfIdf for the context of every link.
	  *
	  * @param articles                   RDD of parsed Wikipedia articles containing the links and their contexts
	  * @param documentFrequencies        RDD of document frequencies
	  * @param numDocuments               number of documents that was used to calculate the document frequencies
	  * @param documentFrequencyThreshold lower threshold for document frequencies
	  * @return RDD of tuples containing the link and the tfIdfs of ever term in its context
	  */
	def calculateLinkContextsTfidf(
		articles: RDD[ParsedWikipediaEntry],
		documentFrequencies: RDD[DocumentFrequency],
		numDocuments: Long,
		documentFrequencyThreshold: Int
	): RDD[(Link, Map[String, Double])] = {
		val contextTermFrequencies = articles
			.flatMap(_.linkswithcontext)
			.map(link => (link, Bag(link.context)))
			.flatMap(t => joinableTermFrequencies(t._1, t._2))

		calculateTfidf(
			contextTermFrequencies,
			documentFrequencies,
			numDocuments,
			documentFrequencyThreshold)
			.reduceByKey(_ ++ _)
	}

	/**
	  * Calculates the tfIdf for every term.
	  *
	  * @param termFrequencies            RDD of terms with their identifier and raw term frequency in the form
	  *                                   (term, (identifier, raw term frequency))
	  * @param documentFrequencies        RDD of Document Frequencies
	  * @param numDocuments               number of documents used to determine the Document Frequencies
	  * @param documentFrequencyThreshold lower threshold used for Document Frequencies of terms without a
	  *                                   precalculated one.
	  * @tparam T type of the identifier for each term
	  * @return RDD of identifiers and a Map containing the tfidf for each term as tuple
	  */
	def calculateTfidf[T](
		termFrequencies: RDD[(String, (T, Int))],
		documentFrequencies: RDD[DocumentFrequency],
		numDocuments: Long,
		documentFrequencyThreshold: Int
	): RDD[(T, Map[String, Double])] = {
		val inverseDocumentFrequencies = documentFrequencies.map(calculateIdf(_, numDocuments))
		termFrequencies.leftOuterJoin(inverseDocumentFrequencies)
			.map { case (token, ((identifier, tf), idfOption)) =>
				val idf = idfOption.getOrElse(calculateIdf(documentFrequencyThreshold - 1, numDocuments))
				val tfidf = tf * idf
				(identifier, Map(token -> tfidf))
			}
	}

	/**
	  * Calculates the Cosine Similarities for every alias and creates feature entries containing the other two
	  * features as well.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val articlesWithTermFrequencies = input.head.asInstanceOf[RDD[ParsedWikipediaEntry]]
		val documentFrequencies = input(1).asInstanceOf[RDD[DocumentFrequency]]
		val numDocuments = articlesWithTermFrequencies.count()

		val articleTfidf = calculateArticleTfidf(
			articlesWithTermFrequencies,
			documentFrequencies,
			numDocuments,
			DocumentFrequencyCounter.leastSignificantDocumentFrequency)

		val contextTfidf = calculateLinkContextsTfidf(
			articlesWithTermFrequencies,
			documentFrequencies,
			numDocuments,
			DocumentFrequencyCounter.leastSignificantDocumentFrequency)

		// TODO: join RDDs and calculate cosine similarity (issue #34)

		Nil
	}
}
