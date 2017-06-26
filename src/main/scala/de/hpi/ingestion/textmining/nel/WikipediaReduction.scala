package de.hpi.ingestion.textmining.nel

import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.{ParsedWikipediaEntry, TrieAliasArticle}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Reduces ParsedWikipediaArticles to relevant attributes for NEL.
  */
object WikipediaReduction extends SparkJob {
	appName = "Wikipedia Reduction"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads Wikipedia entries and aliases from the Cassandra.
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
	  * Save reduced Wikipedia articles to the Cassandra.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		val reducedArticles = output.fromAnyRDD[TrieAliasArticle]().head
		reducedArticles.saveToCassandra(settings("keyspace"), settings("wikipediaNELTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Reduces ParsedWikipediaArticles to relevant attributes for NEL.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val wikipediaArticles = input.head.asInstanceOf[RDD[ParsedWikipediaEntry]]
		val reducedArticles = wikipediaArticles.map(article => TrieAliasArticle(article.title, article.text))
		List(reducedArticles).toAnyRDD()
	}
}
