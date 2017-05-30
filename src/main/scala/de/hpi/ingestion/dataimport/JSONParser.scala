package de.hpi.ingestion.dataimport

import java.text.SimpleDateFormat
import java.util.Date

import play.api.libs.json._

trait JSONParser[T] {
	def parseJSON(line: String): T = {
		val json = Json.parse(line)
		val entity = fillEntityValues(json)
		entity
	}

	def fillEntityValues(json: JsValue): T

	// remove array syntax from JSON for parallel parsing
	def cleanJSON(json: String): String = {
		json.replaceAll("^\\[|,$|, $|\\]$", "")
	}

	def getValue(json: JsValue, path: List[String]): Option[JsValue] = {
		var element = json
		for(pathSegment <- path) {
			if(element.as[JsObject].value.contains(pathSegment)) {
				element = (element \ pathSegment).as[JsValue]
			} else {
				return None
			}
		}
		if(element.getClass == classOf[JsUndefined]) {
			None
		} else {
			Option(element)
		}
	}

	def extractString(json: JsValue, path: List[String]): Option[String] = {
		val value = getValue(json, path)
		value.map(_.as[String])
	}

	def extractList(json: JsValue, path: List[String]): List[JsValue] = {
		val value = getValue(json, path).map(_.as[List[JsValue]])
		value.getOrElse(Nil)
	}

	def extractStringList(json: JsValue, path: List[String]): List[String] = {
		val value = getValue(json, path).map(_.as[List[String]])
		value.getOrElse(Nil)
	}

	def extractMap(json: JsValue, path: List[String]): Map[String, JsValue] = {
		val value = getValue(json, path).map(_.as[Map[String, JsValue]])
		value.getOrElse(Map[String, JsValue]())
	}

	def flattenNestedMap(nestedTuple: (String, JsValue)): List[(String, String)] = {
		val (k, v) = nestedTuple
		v match {
			case t: JsObject => t.as[Map[String, String]]
				.map { case (a, b) => (k + "." + a, b) }.toList
			// TODO: JsArray parsing - when cast to a map only of the tuples in the list will remain
			case t: JsArray => t.as[List[String]].map(el => (k, el))
			case t: JsString => List((k, t.as[String]))
			case _ =>
				throw new Exception(s"Unsupported type: ${v.getClass}")
		}
	}

	def extractDouble(json: JsValue, path: List[String]): Option[String] = {
		val value = getValue(json, path)
		value.map(_.as[Double].toString)
	}

	def extractBoolean(json: JsValue, path: List[String]): Option[Boolean] = {
		val value = getValue(json, path)
		value.map(_.as[Boolean])
	}

	def extractDate(json: JsValue, path: List[String], datePattern: String): Option[Date] = {
		val value = getValue(json, path)
		value.map { dateString =>
			val dateFormat = new SimpleDateFormat(datePattern)
			dateFormat.parse(dateString.as[String])
		}
	}
}
