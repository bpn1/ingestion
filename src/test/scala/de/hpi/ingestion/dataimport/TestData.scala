/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
		List(
			"e. K.",
			"e. V.",
			"Verein",
			"gesellschaft mbH & Co. KG",
			"Gesellschaft GmbH & Co. KG",
			"GmbH Co KG",
			"Gesellschaft mit beschränkter Haftung",
			"Gemeinschaft m.b.H.",
			"Gesellschaft mbh",
			"Gesellschaft bürgerlichen Rechts",
			"Kommanditgesellschaft auf Aktien",
			"aktienGEsellschaft"
		)
	}

	def normalizedLegalForms: List[String] = {
		List(
			"e.K.",
			"e.V.",
			"GmbH & Co. KG",
			"GmbH",
			"GbR",
			"KGaA",
			"AG"
		)
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
