package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models.{ParsedWikipediaEntry, TrieAliasArticle}
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

/**
  * Reduces ParsedWikipediaArticles to relevant attributes for NEL.
  */
object WikipediaNELReduction extends SparkJob {
	appName = "Wikipedia NEL Reduction"
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
		val namedEntities = output.head.asInstanceOf[RDD[TrieAliasArticle]]
		namedEntities.saveToCassandra(settings("keyspace"), settings("wikipediaNELTable"))
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
		val trieAliasArticles = wikipediaArticles.map(article => TrieAliasArticle(article.title, article.text))
		List(trieAliasArticles).toAnyRDD()
	}
}
