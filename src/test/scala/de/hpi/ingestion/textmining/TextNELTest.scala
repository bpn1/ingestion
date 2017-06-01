package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.textmining.models._
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.jsoup.Jsoup
import scala.util.matching.Regex

class TextNELTest extends FlatSpec with SharedSparkContext with Matchers {
	"Found named entities" should "be empty (due to empty classifier model)" in {
		val oldTrieStreamFunctionAliasTrieSearch = AliasTrieSearch.trieStreamFunction
		val testDocFreqFunction = TestData.docfreqStream("docfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction

		val oldTrieStreamFunctionTextNEL = AliasTrieSearch.trieStreamFunction
		val testTrieStreamFunction = TestData.fullTrieStream _
		TextNEL.trieStreamFunction = testTrieStreamFunction

		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val aliases = sc.parallelize(TestData.aliasesWithExistingPagesSet().toList)
		val input = List(articles).toAnyRDD() ++ List(aliases).toAnyRDD()

		TextNEL.modelFunction = TestData.loadRandomForestModel _
		val namedEntities = TextNEL
			.run(input, sc)
			.fromAnyRDD[Link]()
			.head
			.collect
		namedEntities shouldBe empty

		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunctionAliasTrieSearch
		TextNEL.trieStreamFunction = oldTrieStreamFunctionTextNEL
	}

	"Articles with links" should "not be empty" in {
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
			.map(article => Article(article.title, article.getText()))
		val links = sc.parallelize(TestData.linksWithArticleAnnotation().toList)
		val articlesWithLinks = TextNEL.generateArticlesWithLinks(articles, links)
		articlesWithLinks should not be empty
	}

	they should "contain html links" in {
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
			.map(article => Article(article.title, article.getText()))
		val links = sc.parallelize(TestData.linksWithArticleAnnotation().toList)
		val anchorRegex = new Regex("<a href=\".+?\">.+?<\\/a>")
		TextNEL.generateArticlesWithLinks(articles, links)
			.collect
			.foreach { article =>
				if(article.title != "Audi Test ohne Link") {
					anchorRegex.findFirstIn(article.text) should not be empty
				}
			}
	}

	they should "only contain the title and the original text" in {
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
			.map(article => Article(article.title, article.getText()))
		val links = sc.parallelize(TestData.linksWithArticleAnnotation().toList)
		val originalTexts = articles
			.map(article => article.title + "\n" + article.text)
			.collect
			.toSet
		val texts = TextNEL.generateArticlesWithLinks(articles, links)
			.map { article =>
				// Escape newlines with an improbable string. Otherwise Jsoup will replace them with spaces.
				val escapedNewlines = Jsoup.parse(article.text.replaceAll("\n", "#EscapedNewline")).body.text
				escapedNewlines.replaceAll("#EscapedNewline", "\n")
			}
			.collect
			.toSet
		originalTexts shouldEqual texts
	}
}
