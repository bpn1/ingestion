package de.hpi.ingestion.dataimport.kompass

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import play.api.libs.json._
import com.fasterxml.jackson.databind.JsonMappingException

import org.jsoup.Jsoup
import org.jsoup.select._
import org.jsoup.nodes._

object KompassParse {
	val inputFile = "kompass.json"
	// attribute names in JSON
	val providedAttributes = Seq("name", "url", "sector", "specified_sector", "county", "district")
	// attribute names and CSS selectors in HTML
	val attributeSelectors = Map(
		"address" -> ".addressCoordinates",
		"turnover" -> ".turnover li p:eq(1)",
		"employees" -> ".employees li:eq(1) p:eq(1)"
	)

	// remove array syntax from JSON for parallel parsing
	def cleanJSON(json: String): String = {
		json.replaceAll("^\\[|,$|, $|\\]$", "")
	}

	def extractData(line: String): String = {
		val extractedData = ListBuffer[(String, JsValue)]()
		line = cleanJSON(line)
		if(line == "") return
		var json: JsValue = null
		try {
			json = Json.parse(line)
		} catch {
			case jme: JsonMappingException =>
				jme.printStackTrace()
				return "{\"error\": \"JsonMappingException\"}"
		}
		for(attribute <- providedAttributes) {
			extractedData.append((attribute, (json \ attribute)))
		}
		val doc = Jsoup.parse((json \ "html").as[String])
		for((attribute, selector) <- attributeSelectors) {
			val elem = doc.select(selector).first
			if(elem == null) {
				println("Could not find attribute " + attribute + " with selector " + selector)
			} else {
				extractedData.append((attribute, JsString(elem.text.trim)))
			}
		}
		val town = doc.getElementById("search_category_link[3]")
		if(town != null) {
			extractedData.append(("town", JsString(town.text.trim)))
		}

		val executives = ListBuffer[String]()
		for(element <- doc.select("#executives .name")) {
			executives.append(element.text)
		}
		extractedData.append(("executives", Json.toJson(executives.toList)))
		val information = ListBuffer[Tuple2[String, String]]()
		for(element <- doc.select(".general.bloc .presentation.global li")) {
			val texts = element.select("p")
			val title = texts.get(0).text.trim
			val content = texts.get(1).text.trim
			if(content != "")
				information.append((title, content))
		}
		extractedData.append(("information", Json.toJson(information.toMap)))
		val activities = ListBuffer[String]()
		for(element <- doc.select("#secondaryActivitiesTree a")) {
			activities.append(element.text.replace("\"","").trim)
		}
		extractedData.append(("activities", Json.toJson(activities.toList)))
		Json.stringify(Json.toJson(extractedData.toMap))
	}

	def main(args : Array[String]) {
		val sc = new SparkContext(new SparkConf().setAppName("KompassParse"))
		val jsonFile = sc.textFile(inputFile)
		val words = jsonFile
			.map(extractData)
			.saveAsTextFile("kompass_parse_" + System.currentTimeMillis / 1000)

		sc.stop()
	}
}
