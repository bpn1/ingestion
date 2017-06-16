package de.hpi.ingestion.dataimport

import play.api.libs.json.{JsValue, Json}
import scala.io.Source

object TestData {
	def validURLs: List[String] = {
		List("http://google.com", "https://www.facebook.com", "http://www.youtube.de")
	}

	def invalidURLs: List[String] = {
		List("www.youtube.de", "isbn://1337", "NotAURL", "about:black", "youtube.com")
	}

	def unnormalizedLegalForms: List[String] = {
		List("e. K.", "e. V.", "gesellschaft mbH & Co. KG", "gesellschaft GmbH & Co. KG")
	}

	def normalizedLegalForms: List[String] = {
		List("e.K.", "e.V.", "GmbH & Co. KG")
	}

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
