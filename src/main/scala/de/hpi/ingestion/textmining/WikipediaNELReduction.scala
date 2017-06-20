package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models.{Link, ParsedWikipediaEntry, TrieAliasArticle}
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

/**
  * todo
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
	  * Saves found links to named entities to the HDFS.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		val namedEntities = output.head.asInstanceOf[RDD[TrieAliasArticle]]
		namedEntities.saveToCassandra(settings("keyspace"), settings("namedEntitiesTable"))
	}

	/**
	  * Loads the random forest classifier model from the HDFS.
	  *
	  * @param sc SparkContext used for loading from the HDFS
	  * @return random forest classifier model
	  */
	def loadModel(sc: SparkContext): RandomForestModel = {
		RandomForestModel.load(sc, settings("classifierModel"))
	}

	/**
	  * #todo
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val wikipediaArticles = input.head.asInstanceOf[RDD[ParsedWikipediaEntry]]
		val trieAliasArticles = wikipediaArticles.map(article => TrieAliasArticle(article.title, article.text))
		List(trieAliasArticles).toAnyRDD()
	}

	// $COVERAGE-ON$
}
