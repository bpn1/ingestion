package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.{Alias, ParsedWikipediaEntry}
import com.datastax.spark.connector._
import de.hpi.ingestion.textmining.CosineContextComparator._
import de.hpi.ingestion.textmining.tokenizer.{CoreNLPTokenizer, IngestionTokenizer}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.IDF

/**
  * Compares the performance of the context extraction and tf-idf computation with the implementation in the
  * Spark MLlib.
  */
object ContextPerformanceProfiler extends SparkJob {
	appName = "Context Performance Profiler"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries with their term frequencies and the document frequencies from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val tfArticles = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		List(tfArticles).toAnyRDD()
	}

	/**
	  * #todo
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[List[(String, Long)]]()
			.head
			.saveAsTextFile(s"context_performance_profile${System.currentTimeMillis()}")
	}

	// $COVERAGE-ON$

	/**
	  * #todo
	  * https://spark.apache.org/docs/1.2.0/mllib-feature-extraction.html
	  */
	def sparkMLlibTfidfComputation(tokensPerDocument: RDD[List[String]], sc: SparkContext): RDD[Vector] = {
		val hashingTF = new HashingTF()
		val tf: RDD[Vector] = hashingTF.transform(tokensPerDocument)
		tf.cache()
		val idf = new IDF(minDocFreq = DocumentFrequencyCounter.leastSignificantDocumentFrequency).fit(tf)
		val tfidf = idf.transform(tf)
		tfidf
	}

	/**
	  * #todo
	  *
	  * @param articlesWithTermFrequencies
	  * @param sc
	  * @tparam T
	  * @return
	  */
	def ingestionTfidfComputation[T](
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
	  * #todo
	  * https://stackoverflow.com/a/35290316
	  *
	  * @param function
	  * @tparam T
	  * @return microsecs
	  */
	def profileTime[T](function: => T): Long = {
		val start = System.nanoTime()
		val res = function
		val end = System.nanoTime()
		(end - start) / 1000
	}

	/**
	  * #todo
	  *
	  * @param articles
	  * @return articles with term frequencies
	  */
	def extractSimpleContexts(articles: RDD[ParsedWikipediaEntry]): RDD[(String, List[String])] = {
		val tokenizer = IngestionTokenizer()
		articles.map(article => (article.title, tokenizer.process(article.getText())))
	}

	/**
	  * Compares the performance of the context extraction and tf-idf computation with the implementation in the
	  * Spark MLlib.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to #todo
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val articlesWithMapContexts = input.head.asInstanceOf[RDD[ParsedWikipediaEntry]] //.sample(0.01)

		val articlesWithListContexts = extractSimpleContexts(articlesWithMapContexts)
		val tokensPerDocument = articlesWithListContexts.map(_._2)
		val timeSpark = profileTime(sparkMLlibTfidfComputation(tokensPerDocument, sc))
		val timeIngestion = profileTime(ingestionTfidfComputation(articlesWithMapContexts, sc))

		val statistics = List[(String, Long)](("SparkMLlib", timeSpark), ("Ingestion", timeIngestion))
		List(sc.parallelize(statistics)).toAnyRDD()
	}
}
