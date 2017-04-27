package de.hpi.ingestion.dataimport

import java.text.SimpleDateFormat
import java.util.Date

import play.api.libs.json._

abstract class JSONParser[T] {
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
		var element = Option(json)
		try {
			for (pathSegment <- path) {
				element = Option((element.get \ pathSegment).as[JsValue])
			}
		} catch {
			case jre: JsResultException =>
				element = None
			case ex: NoSuchElementException =>
				element = None
		}

		if (element.isDefined && element.get.getClass == classOf[JsUndefined]) {
			element = None
		}

		element
	}

	def extractString(json: JsValue, path: List[String]): Option[String] = {
		val value = getValue(json, path)
		if (value.isEmpty) {
			return None
		}
		value.orNull.asOpt[String]
	}

	def extractList(json: JsValue, path: List[String]): List[JsValue] = {
		val value = getValue(json, path).getOrElse(JsArray())
		value.as[List[JsValue]]
	}

	def extractStringList(json: JsValue, path: List[String]): List[String] = {
		val value = getValue(json, path).getOrElse(JsArray())
		value.as[List[String]]
	}

	def extractMap(json: JsValue, path: List[String]): Map[String, JsValue] = {
		val value = getValue(json, path).getOrElse(JsObject(Seq[(String, JsValue)]()))
		value.as[Map[String, JsValue]]
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
		value.orNull.asOpt[Double].map(_.toString)
	}

	def extractBoolean(json: JsValue, path: List[String]): Option[Boolean] = {
		val value = getValue(json, path)
		value.orNull.asOpt[Boolean]
	}

	def extractDate(json: JsValue, path: List[String], datePattern: String): Option[Date] = {
		val value = getValue(json, path)
		if (value.isDefined) {
			val dateFormat = new SimpleDateFormat(datePattern)
			val date = dateFormat.parse(value.get.as[String])
			Option(date)
		} else {
			None
		}
	}
}
