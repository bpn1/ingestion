package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.{Alias, ParsedWikipediaEntry}
import com.datastax.spark.connector._
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
		val aliases = sc.cassandraTable[Alias](settings("keyspace"), settings("linkTable"))
		List(tfArticles).toAnyRDD() ++ List(aliases).toAnyRDD()
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
			.fromAnyRDD[List[(String, Double)]]()
			.head
			.saveAsTextFile(s"context_performance_profile${System.currentTimeMillis()}")
	}

	// $COVERAGE-ON$

	/**
	  * #todo
	  * https://spark.apache.org/docs/1.2.0/mllib-feature-extraction.html
	  */
	def sparkMLlibContextExtraction(documents: RDD[Seq[String]], sc: SparkContext): RDD[Vector] = {
		val hashingTF = new HashingTF()
		val tf: RDD[Vector] = hashingTF.transform(documents)
		tf.cache()
		val idf = new IDF(minDocFreq = DocumentFrequencyCounter.leastSignificantDocumentFrequency).fit(tf)
		idf.transform(tf)
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
		val articlesWithTermFrequencies = input.head.asInstanceOf[RDD[ParsedWikipediaEntry]] //.sample(0.01)
		val aliases = input(1).asInstanceOf[RDD[Alias]]

		val articles = articlesWithTermFrequencies.map(_.getText().split(" ").toSeq)
		sparkMLlibContextExtraction(articles, sc)

		val statistics = List[(String, Double)](("SparkMLlib", 1234.567))
		List(sc.parallelize(statistics)).toAnyRDD()
	}
}
