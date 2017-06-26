package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.ParsedWikipediaEntry
import de.hpi.ingestion.textmining.CosineContextComparator._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.IDF
import com.datastax.spark.connector._
import de.hpi.ingestion.textmining.TermFrequencyCounter.extractArticleContext

/**
  * Compares the performance of the context extraction and tf-idf computation of the Ingestion project with the
  * implementation in the Spark MLlib.
  */
object ContextPerformanceProfiler extends SparkJob {
	appName = "Context Performance Profiler"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia Entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val articles = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		List(articles).toAnyRDD()
	}

	/**
	  * Saves profiled runtimes to the HDFS.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[List[(String, Long)]]()
			.head
			.saveAsTextFile(s"context_tfidf_runtimes_${System.currentTimeMillis()}")
	}
	// $COVERAGE-ON$

	/**
	  * Extracts contexts from articles as lists of words.
	  *
	  * @param articles  all articles
	  * @param tokenizer word tokenizer
	  * @return list of words for each article
	  */
	def contextExtractionForSparkMLlib(
		articles: RDD[ParsedWikipediaEntry],
		tokenizer: IngestionTokenizer
	): RDD[List[String]] = {
		articles.map(article => tokenizer.process(article.getText()))
	}

	/**
	  * Extracts contexts from articles as bags of words.
	  *
	  * @param articles  all articles
	  * @param tokenizer word tokenizer
	  * @return bag of words for each article
	  */
	def contextExtractionForIngestion(
		articles: RDD[ParsedWikipediaEntry],
		tokenizer: IngestionTokenizer
	): RDD[ParsedWikipediaEntry] = {
		articles.map(extractArticleContext(_, tokenizer))
	}

	/**
	  * Calculates tfidf values by means of the SparkMLlib.
	  * https://spark.apache.org/docs/1.2.0/mllib-feature-extraction.html
	  *
	  * @param wordsPerArticle each article as a list of words
	  * @return tfidf values
	  */
	def sparkMLlibTfidfComputation(wordsPerArticle: RDD[List[String]], sc: SparkContext): RDD[Vector] = {
		val hashingTF = new HashingTF()
		val tf: RDD[Vector] = hashingTF.transform(wordsPerArticle)
		tf.cache()
		val idf = new IDF(minDocFreq = DocumentFrequencyCounter.leastSignificantDocumentFrequency).fit(tf)
		idf.transform(tf)
	}

	/**
	  * Calculates tfidf values by means of the Ingestion project.
	  *
	  * @param articlesWithTermFrequencies each article enriched with its context
	  * @param sc                          spark context
	  * @return tfidf values
	  */
	def ingestionTfidfComputation(
		articlesWithTermFrequencies: RDD[ParsedWikipediaEntry],
		sc: SparkContext
	): RDD[(String, Map[String, Double])] = {
		val numDocuments = articlesWithTermFrequencies.count()
		val defaultDf = calculateDefaultDf(DocumentFrequencyCounter.leastSignificantDocumentFrequency)
		val defaultIdf = calculateIdf(defaultDf, numDocuments)
		val settingsBroadcast = sc.broadcast(settings)
		val articleTfs = transformArticleTfs(articlesWithTermFrequencies)
		calculateTfidf(articleTfs, numDocuments, defaultIdf, settingsBroadcast.value)
	}

	/**
	  * Measures the runtime of a given function.
	  * https://stackoverflow.com/a/35290316
	  *
	  * @param function callback function
	  * @tparam T output type of the function
	  * @return runtime in milliseconds
	  */
	def profileTime[T](function: => T): (T, Long) = {
		val start = System.nanoTime()
		val res = function
		val end = System.nanoTime()
		(res, (end - start) / 1000000)
	}

	/**
	  * Compares the performance of the context extraction and tf-idf computation of the Ingestion project with the
	  * implementation in the Spark MLlib.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val articles = sc.parallelize(input.head.asInstanceOf[RDD[ParsedWikipediaEntry]].take(10000))
		val tokenizer = IngestionTokenizer(args)

		val (wordsPerArticle, timeSparkContext) = profileTime(contextExtractionForSparkMLlib(articles, tokenizer))
		val (contextArticles, timeIngestionContext) = profileTime(contextExtractionForIngestion(articles, tokenizer))
		val (sparkTfidf, timeSparkTfidf) = profileTime(sparkMLlibTfidfComputation(wordsPerArticle, sc))
		val (ingestionTfidf, timeIngestionTfidf) = profileTime(ingestionTfidfComputation(contextArticles, sc))

		val statistics = List[(String, Long)](
			("SparkMLlib Context Extraction", timeSparkContext),
			("Ingestion Context Extraction", timeIngestionContext),
			("SparkMLlib Tfidf Computation", timeSparkTfidf),
			("Ingestion Tfidf Computation", timeIngestionTfidf),
			("SparkMLlib total", timeSparkContext + timeSparkTfidf),
			("Ingestion total", timeIngestionTfidf + timeIngestionTfidf)
		)
		List(sc.parallelize(statistics)).toAnyRDD()
	}
}
