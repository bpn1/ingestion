package de.hpi.ingestion.dataimport.kompass

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import play.api.libs.json._
import com.datastax.spark.connector._
import com.fasterxml.jackson.databind.JsonMappingException
import org.apache.spark.rdd.RDD
import org.jsoup.Jsoup
import de.hpi.ingestion.dataimport.JSONParser
import de.hpi.ingestion.dataimport.kompass.models.KompassEntity
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.versioncontrol.VersionDiff.{keyspace, tablename}
import de.hpi.ingestion.implicits.CollectionImplicits._

object KompassParse extends SparkJob with JSONParser[KompassEntity]{
	appName = "Kompass Parse"
	configFile = "textmining.xml"
	val inputFile = "kompass.json"

	val keyspace = "datalake"
	val outputTablename = "kompass_entities"

	// attribute names in JSON
	val providedAttributes: Seq[String] = Seq(
		"name",
		"url",
		"sector",
		"specified_sector",
		"county",
		"district"
	)
	// attribute names and CSS selectors in HTML
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

	def extractData(line: String): Option[Map[String, List[String]]] = {
		var extractedData: Map[String, List[String]] = Map()
		val row: Option[String] = Some(cleanJSON(line))
		if(row.isEmpty) return None
		var json: JsValue = null
		try {
			json = Json.parse(line)
		} catch {
			case jme: JsonMappingException =>
				jme.printStackTrace()
				return None
		}
		providedAttributes.foreach(attribute => extractedData += attribute -> List((json \ attribute).as[String]))

		val doc = Jsoup.parse((json \ "html").as[String])
		for((attribute, selector) <- attributeSelectors) {
			val elem = doc.select(selector).first
			if(elem == null) {
				println("Could not find attribute " + attribute + " with selector " + selector)
			} else {
				extractedData += attribute -> List(elem.text.trim)
			}
		}
		val city = doc.getElementById("search_category_link[3]")
		if(city != null) {
			extractedData += "city" -> List(city.text.trim)
		}
		val executives = ListBuffer[String]()
		for(element <- doc.select("#executives .name")) {
			executives.append(element.text)
		}
		extractedData += "executives" -> executives.toList

		for(element <- doc.select(".general.bloc .presentation.global li")) {
			val texts = element.select("p")
			val title = texts.get(0).text.trim
			val content = texts.get(1).text.trim
			if(content != "") {
				extractedData += title -> List(content)
			}
		}

		val activities = ListBuffer[String]()
		for(element <- doc.select("#secondaryActivitiesTree a")) {
			activities.append(element.text.replace("\"","").trim)
		}
		extractedData += "activities" -> activities.toList

		Option(extractedData)
	}

	def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		input
			.fromAnyRDD[String]()
			.map(_.flatMap(extractData))
			.toAnyRDD()
	}
}
