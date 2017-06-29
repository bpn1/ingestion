package de.hpi.ingestion.dataimport.kompass

import scala.collection.JavaConversions._
import org.apache.spark.SparkContext
import play.api.libs.json._
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.jsoup.Jsoup
import de.hpi.ingestion.dataimport.JSONParser
import de.hpi.ingestion.dataimport.kompass.models.KompassEntity
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._

object KompassParse extends SparkJob with JSONParser[KompassEntity]{
	appName = "Kompass Parse"
	configFile = "textmining.xml"
	val inputFile = "kompass.json"
	val keyspace = "datalake"
	val outputTablename = "kompass_entities"

	val providedAttributes: Seq[String] = Seq(
		"url",
		"sector",
		"specified_sector",
		"county",
		"district"
	)
	val attributeSelectors = Map(
		"address" -> ".addressCoordinates",
		"turnover" -> ".turnover li p:eq(1)",
		"employees" -> ".employees li:eq(1) p:eq(1)"
	)

	// $COVERAGE-OFF$
	/**
	  * Loads JSON from the HDFS.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val kompassPages = sc.textFile(inputFile)
		List(kompassPages.asInstanceOf[RDD[Any]])
	}

	/**
	  * Writes the JSON diff to the HDFS.
	  * @param output first element is the RDD of JSON diffs
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[KompassEntity]()
			.head
			.saveToCassandra(keyspace, outputTablename)
	}
	// $COVERAGE-ON$
	/**
	  * Extracts data from the input json and maps them to a Kompass entity.
	  * @param json JSON-Object containing the data
	  * @return KompassEntity containing the parsed data
	  */
	override def fillEntityValues(json: JsValue): KompassEntity = {
		var extractedData: Map[String, List[String]] = Map()
		providedAttributes.foreach(attribute => extractedData += attribute -> List((json \ attribute).as[String]))
		val name = getValue(json, List("name")).map(_.as[String])

		val doc = Jsoup.parse((json \ "html").as[String])
		attributeSelectors.foreach { case (attribute, selector) =>
			val elem = doc.select(selector).first
			if (elem == null) {
				println("Could not find attribute " + attribute + " with selector " + selector)
			} else {
				extractedData += attribute -> List(elem.text.trim)
			}
		}

		val city = doc.getElementById("search_category_link[3]")
		if(city != null) { extractedData += "city" -> List(city.text.trim) }

		var executives = List[String]()
		doc.select("#executives .name").foreach(x => executives :::= List(x.text))
		extractedData += "executives" -> executives

		doc.select(".general.bloc .presentation.global li").foreach { element =>
			val texts = element.select("p")
			if (texts.length > 1) {
				val title = texts.get(0).text.trim
				val content = texts.get(1).text.trim
				if (content != "") {
					extractedData += title -> List(content)
				}
			}
		}

		var activities = List[String]()
		doc.select("#secondaryActivitiesTree a").foreach(x => activities :::= List(x.text.replace("\"","").trim))
		extractedData += "activities" -> activities

		KompassEntity(name, Option("company"), extractedData)
	}

	/**
	  * Extracts the information from the json containing the kompass data.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		input
			.fromAnyRDD[String]()
			.map(_.map(cleanJSON))
			.map(_.collect { case line: String if line.nonEmpty => parseJSON(line) })
			.toAnyRDD()
	}
}
