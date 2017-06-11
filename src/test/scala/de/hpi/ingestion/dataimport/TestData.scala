package de.hpi.ingestion.dataimport

import play.api.libs.json.{JsValue, Json}

import scala.io.Source

object TestData {

	def parsedJSON(): JsValue = {
		Json.parse(Source.fromURL(getClass.getResource("/parsing.json")).getLines().mkString("\n"))
	}

	def dirtyJsonLines(): List[String] = {
		List(
			"[",
			"{a},",
			"{b},",
			"]",
			"[{c},",
			"{d}]"
		)
	}

	def cleanenedJsonLines(): List[String] = {
		List(
			"",
			"{a}",
			"{b}",
			"",
			"{c}",
			"{d}"
		)
	}
}
