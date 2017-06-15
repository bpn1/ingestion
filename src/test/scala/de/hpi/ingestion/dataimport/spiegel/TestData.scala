package de.hpi.ingestion.dataimport.spiegel

import de.hpi.ingestion.dataimport.spiegel.models.SpiegelArticle
import de.hpi.ingestion.textmining.models.TrieAlias
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

	def parsedAndSearchedArticles(): List[SpiegelArticle] = {
		List(
			SpiegelArticle(
				id = "spiegel id 1",
				url = Option("spiegel.de/test1"),
				title = Option("test title 1"),
				text = Option("test title 1 test body 1"),
				triealiases = List(TrieAlias("test title", Option(0)))),
			SpiegelArticle(
				id = "spiegel id 2",
				url = Option("spiegel.de/test2"),
				title = Option("test title 2"),
				text = Option("test body 2")),
			SpiegelArticle(
				id = "spiegel id 3",
				url = None,
				title = Option("test title 3"),
				text = Option("test title 3"),
				triealiases = List(TrieAlias("test title", Option(0)))),
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

	def aliasSearchArticles(): List[SpiegelArticle] = {
		List(
			SpiegelArticle(
				id = "1",
				text = Option("Dieser Artikel schreibt über Audi.")
			),
			SpiegelArticle(
				id = "2",
				text = Option("Die Audi AG ist ein Tochterunternehmen der Volkswagen AG.")
			),
			SpiegelArticle(
				id = "3",
				text = Option("Die Volkswagen Aktiengesellschaft ist nicht sehr umweltfreundlich.")
			),
			SpiegelArticle(
				id = "4",
				text = Option("Dieser Satz enthält Audi. Dieser Satz enthält Audi AG. Dieser Satz enthält Volkswagen.")
			)
		)
	}

	def foundAliasArticles(): List[SpiegelArticle] = {
		List(
			SpiegelArticle(
				id = "1",
				text = Option("Dieser Artikel schreibt über Audi."),
				triealiases = List(
					TrieAlias("Audi", Option(29))
				)
			),
			SpiegelArticle(
				id = "2",
				text = Option("Die Audi AG ist ein Tochterunternehmen der Volkswagen AG."),
				triealiases = List(
					TrieAlias("Audi AG", Option(4)),
					TrieAlias("Volkswagen AG", Option(43))
				)
			),
			SpiegelArticle(
				id = "3",
				text = Option("Die Volkswagen Aktiengesellschaft ist nicht sehr umweltfreundlich."),
				triealiases = List(
					TrieAlias("Volkswagen", Option(4))
				)
			),
			SpiegelArticle(
				id = "4",
				text = Option("Dieser Satz enthält Audi. Dieser Satz enthält Audi AG. Dieser Satz enthält Volkswagen."),
				triealiases = List(
					TrieAlias("Audi", Option(20)),
					TrieAlias("Audi AG", Option(46)),
					TrieAlias("Volkswagen", Option(75))
				)
			)
		)
	}
}
