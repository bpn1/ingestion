package de.hpi.ingestion.textmining

import java.io.{BufferedReader, InputStreamReader}

import de.hpi.ingestion.textmining.models._
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._

import scala.collection.mutable

/**
  * Generates feature entries for all occurring pairs of aliases and pages they may refer to.
  */
object CosineContextComparator extends SparkJob {
	appName = "Cosine Context Comparator"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries with their term frequencies and loads the document frequencies from the
	  * Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val tfArticles = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		val documentFrequencies = sc.cassandraTable[DocumentFrequency](settings("keyspace"), settings("dfTable"))
		val aliases = sc.cassandraTable[Alias](settings("keyspace"), settings("linkTable"))
		List(tfArticles).toAnyRDD() ++ List(documentFrequencies).toAnyRDD() ++ List(aliases).toAnyRDD()
	}

	/**
	  * Saves the Feature Entries to the Cassandra.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[FeatureEntry]()
			.head
			.saveToCassandra(settings("keyspace"), settings("featureTable"))
	}
	// $COVERAGE-ON$

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
		Math.log10(numDocs.toDouble / df.toDouble)
	}

	/**
	  * Returns default value for the Document Frequency used in the idf calculation.
	  * @param threshold least significant Document Frequency
	  * @return default Document Frequency used for idf calculation
	  */
	def calculateDefaultDf(threshold: Int = DocumentFrequencyCounter.leastSignificantDocumentFrequency): Int = {
		threshold - 1
	}

	/**
	  * Transforms articles into a format needed to calculate the tfIdf for every article.
	  *
	  * @param articles                   RDD of parsed Wikipedia articles with known term frequencies
	  * @return RDD of tuples containing the page name and the tfIdfs of every term on that page
	  */
	def transformArticleTfs(articles: RDD[ParsedWikipediaEntry]): RDD[(String, Bag[String, Int])] = {
		articles.map(entry => (entry.title, Bag(entry.context)))
	}

	/**
	  * Transforms links into a format needed to calculate the tfIdf for every link.
	  *
	  * @param articles                   RDD of parsed Wikipedia articles containing the links and their contexts
	  * @return RDD of tuples containing the link and the tfIdfs of every term in its context
	  */
	def transformLinkContextTfs(articles: RDD[ParsedWikipediaEntry]): RDD[(Link, Bag[String, Int])] = {
		articles
			.flatMap(_.linkswithcontext)
			.map(link => (link, Bag(link.context)))
	}

	/**
	  * Calculates the tfIdf for every term.
	  * @param termFrequencies RDD of terms with their identifier and raw term frequency in the form
	  *                        (term, (identifier, raw term frequency))
	  * @param numDocuments number of documents used to determine the Document Frequencies
	  * @param defaultIdf default idf value for terms without a document frequency above the threshold
	  * @tparam T type of the identifier for each term
	  * @return RDD of tuples containing identifiers and a Map with the tfidf for each term
	  */
	def calculateTfidf[T](
		termFrequencies: RDD[(T, Bag[String, Int])],
		numDocuments: Long,
		defaultIdf: Double
	): RDD[(T, Map[String, Double])] = {
		termFrequencies
			.mapPartitions({ partition =>
				val idfMap = inverseDocumentFrequencies(numDocuments)
				partition.map { case (identifier, bagOfWords) =>
					val idfs = bagOfWords.getCounts().map { case (token, tf) =>
						val idf = idfMap.getOrElse(token, defaultIdf)
						val tfidf = tf * idf
						(token, tfidf)
					}
					(identifier, idfs)
				}
			}, true)
	}

	/**
	  * Computes probability that alias is a link and that it refers to a specific page for all aliases.
	  *
	  * @param aliases aliases with their occurrence frequencies and pages they may refer to
	  * @return aliases with probability features
	  */
	def computeAliasProbabilities(aliases: RDD[Alias]): RDD[(String, (String, Double, Double))] = {
		aliases
			.filter(alias => alias.linkoccurrences.isDefined && alias.totaloccurrences.isDefined)
			.flatMap { alias =>
				val totalProb = alias.linkoccurrences.get.toDouble / alias.totaloccurrences.get.toDouble
				val numPages = alias.pages.values.sum.toDouble
				val pageProb = alias.pages.mapValues(_.toDouble / numPages)
				alias.pages.keySet.map(page => (alias.alias, (page, totalProb, pageProb(page))))
			}
	}

	/** Adds the cosine similarity feature value to the alias probability feature values.
	  *
	  * @param linkContexts           links with their contexts
	  * @param aliasPageProbabilities both alias probabilities feature values
	  * @param articleContexts        article names with their bags of words
	  * @return feature entries containing alias probabilities and cosine similarity
	  */
	def compareLinksWithArticles(
		linkContexts: RDD[(Link, Map[String, Double])],
		aliasPageProbabilities: RDD[(String, (String, Double, Double))],
		articleContexts: RDD[(String, Map[String, Double])]
	): RDD[FeatureEntry] = {
		val joinableLinks = linkContexts
			.map(t => (t._1.alias, t))
			.join(aliasPageProbabilities)
			.map { case (alias, ((link, linkContext), (page, totalProb, pageProb))) =>
				(page, List(ProtoFeatureEntry(alias, link, linkContext, totalProb, pageProb)))
			}.reduceByKey(_ ++ _)
		val weights = (0 until 10).map(t => 1.0).toArray
		val linkSegements = joinableLinks.randomSplit(weights)
		linkSegements.map { rdd =>
			rdd
				.join(articleContexts)
				.flatMap { case (page, (protoFeatureEntries, articleContext)) =>
					protoFeatureEntries.map { protoFeatureEntry =>
							val cosineSim = calculateCosineSimilarity(protoFeatureEntry.linkContext, articleContext)
							protoFeatureEntry.toFeatureEntry(page, cosineSim)
					}
				}
		}.reduce(_.union(_))
	}

	/**
	  * Calculates the length of a vector.
	  *
	  * @param vector vector
	  * @return length
	  */
	def calculateLength(vector: Iterable[Double]): Double = {
		math.sqrt(vector.map(t => t * t).sum)
	}

	/**
	  * Calculate the cosine similarity between two vectors with possibly different dimensions.
	  *
	  * @param vectorA vector with dimension keys
	  * @param vectorB vector with dimension keys
	  * @return cosine similarity
	  */
	def calculateCosineSimilarity[T](vectorA: Map[T, Double], vectorB: Map[T, Double]): Double = {
		val lengthA = calculateLength(vectorA.values)
		val lengthB = calculateLength(vectorB.values)
		val dotProduct = vectorA.keySet.intersect(vectorB.keySet)
			.map(key => vectorA(key) * vectorB(key))
			.sum
		dotProduct / (lengthA * lengthB)
	}

	/**
	  * Reads the document frequencies from the hdfs, calculates the inverse document frequencies and returns them
	  * as Map.
	  * @param numDocs number of documents used to create the document frequency data
	  * @return Map of every term and its inverse document frequency
	  */
	def inverseDocumentFrequencies(numDocs: Long): Map[String, Double] = {
		val docfreqStream = AliasTrieSearch.trieStreamFunction(settings("dfFile"))
		val reader = new BufferedReader(new InputStreamReader(docfreqStream, "UTF-8"))
		val idfMap = mutable.Map[String, Double]()
		var line = reader.readLine()
		while(line != null) {
			val Array(count, token) = line.split("\t", 2)
			idfMap(token) = calculateIdf(count.toInt, numDocs)
			line = reader.readLine()
		}
		idfMap.toMap
	}

	/**
	  * Calculates the Cosine Similarities for every alias and creates feature entries containing the other two
	  * features as well.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val articlesWithTermFrequencies = input.head.asInstanceOf[RDD[ParsedWikipediaEntry]].cache
		val aliases = input(1).asInstanceOf[RDD[Alias]]

		val numDocuments = articlesWithTermFrequencies.count()
		val defaultDf = calculateDefaultDf(DocumentFrequencyCounter.leastSignificantDocumentFrequency)
		val defaultIdf = calculateIdf(defaultDf, numDocuments)

		val articleTfs = transformArticleTfs(articlesWithTermFrequencies)
		val articleTfidf = calculateTfidf(articleTfs, numDocuments, defaultIdf)

		val contextTfs = transformLinkContextTfs(articlesWithTermFrequencies)
		val contextTfidf = calculateTfidf(contextTfs, numDocuments, defaultIdf)

		val aliasPageProbabilities = computeAliasProbabilities(aliases)
		val featureEntries = compareLinksWithArticles(contextTfidf, aliasPageProbabilities, articleTfidf)
		List(featureEntries).toAnyRDD()
	}
}
