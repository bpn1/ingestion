package de.hpi.ingestion.dataimport.spiegel

import de.hpi.ingestion.dataimport.spiegel.models.SpiegelArticle
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.jsoup.Jsoup
import play.api.libs.json.JsValue
import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.JSONParser

/**
  * Parses the Spiegel JSON dump to Spiegel Articles, parses the HTML to raw text and saves them to the Cassandra.
  */
object SpiegelImport extends SparkJob with JSONParser[SpiegelArticle] {
	appName = "Spiegel Import"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads the Spiegel JSON dump from the HDFS.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val spiegel = sc.textFile(settings("spiegelFile"))
		List(spiegel).toAnyRDD()
	}

	/**
	  * Saves the parsed Spiegel Articles to the cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[SpiegelArticle]()
			.head
			.saveToCassandra(settings("keyspace"), settings("spiegelTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Parses the Spiegel JSON dump into Spiegel Articles.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		input
			.fromAnyRDD[String]()
			.map(_.map(parseJSON))
			.toAnyRDD()
	}

	/**
	  * Extracts the article data from a given JSON object and parses the HTML content into text.
	  * @param json JSON object containing the article data
	  * @return Spiegel Article containing the parsed JSON data
	  */
	override def fillEntityValues(json: JsValue): SpiegelArticle = {
		val id = extractString(json, List("_id", "$oid")).get
		val url = extractString(json, List("url"))
		val title = extractString(json, List("title"))
		val content = extractString(json, List("content"))
		val text = content
			.map(_.replaceAll("&nbsp;", " "))
			.map(Jsoup.parse)
			.map(htmlDoc => s"${htmlDoc.head().text()} ${htmlDoc.body().text()}".trim)
		SpiegelArticle(id, title, text, url)
	}
}
