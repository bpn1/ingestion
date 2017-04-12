package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

object CosineContextComparator {
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"
	val inputDocumentFrequenciesTablename = "wikipediadocfreq"

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
	  * Calculates the inverse document frequency for a given document frequency and the number of documents used to
	  * calculate it.
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
	  * Calculates the tfidf of every term for every Wikipedia article.
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

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Cosine Context Comparator")

		val sc = new SparkContext(conf)
		val articlesWithTermFrequencies = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		val documentFrequencies = sc.cassandraTable[DocumentFrequency](keyspace, inputDocumentFrequenciesTablename)

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

		sc.stop()
	}
}
