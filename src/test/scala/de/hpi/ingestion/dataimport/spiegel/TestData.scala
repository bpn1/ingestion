package de.hpi.ingestion.dataimport.spiegel

import de.hpi.ingestion.dataimport.spiegel.models.SpiegelArticle
import play.api.libs.json.{JsObject, Json}
import scala.io.Source

object TestData {

	def spiegelFile(): List[String] = {
		Source.fromURL(getClass.getResource("/spiegel/spiegel.json"))
			.getLines()
			.toList
	}

	def spiegelJson(): List[JsObject] = {
		spiegelFile()
			.map(Json.parse)
			.map(_.as[JsObject])
	}

	def parsedArticles(): List[SpiegelArticle] = {
		List(
			SpiegelArticle(
				id = "spiegel id 1",
				url = Option("spiegel.de/test1"),
				title = Option("test title 1"),
				text = Option("test title 1 test body 1")),
			SpiegelArticle(
				id = "spiegel id 2",
				url = Option("spiegel.de/test2"),
				title = Option("test title 2"),
				text = Option("test body 2")),
			SpiegelArticle(
				id = "spiegel id 3",
				url = None,
				title = Option("test title 3"),
				text = Option("test title 3")),
			SpiegelArticle(
				id = "spiegel id 4",
				url = Option("spiegel.de/test4"),
				title = None,
				text = Option("abc")),
			SpiegelArticle(
				id = "spiegel id 5",
				url = Option("spiegel.de/test5"),
				title = Option("test title 5"),
				text = None))
	}
}
