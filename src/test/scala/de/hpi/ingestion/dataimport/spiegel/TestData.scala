package de.hpi.ingestion.dataimport.spiegel

import de.hpi.ingestion.textmining.models.TrieAliasArticle
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

	def spiegelPages(): List[String] = {
		List(
			"""<div class="spArticleContent">test tag 1</div> abc""",
			"""<div class="dig-artikel">test tag 2</div> abc""",
			"""<div class="article-section">test tag 3</div> abc""",
			"""test no tag"""
		)
	}

	def pageTexts(): List[String] = {
		List(
			"test tag 1",
			"test tag 2",
			"test tag 3",
			"test no tag"
		)
	}

	def parsedArticles(): List[TrieAliasArticle] = {
		List(
			TrieAliasArticle(
				article = "spiegel id 1",
				text = Option("test title 1 test body 1")),
			TrieAliasArticle(
				article = "spiegel id 2",
				text = Option("test body 2")),
			TrieAliasArticle(
				article = "spiegel id 3",
				text = Option("test title 3")),
			TrieAliasArticle(
				article = "spiegel id 4",
				text = Option("abc")),
			TrieAliasArticle(
				article = "spiegel id 5",
				text = None))
	}
}
