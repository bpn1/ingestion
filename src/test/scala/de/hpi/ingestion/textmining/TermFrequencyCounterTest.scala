package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.ParsedWikipediaEntry
import de.hpi.ingestion.textmining.tokenizer.{CleanCoreNLPTokenizer, IngestionTokenizer}

class TermFrequencyCounterTest extends FlatSpec with SharedSparkContext with Matchers {

	"Term frequencies" should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		val pagesTermFrequencies = articles
			.map(TermFrequencyCounter.extractBagOfWords(_, IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)))
		pagesTermFrequencies should not be empty
		pagesTermFrequencies
			.map(_.getCounts())
			.collect
			.foreach(bag => bag should not be empty)
	}

	they should "be exactly these term frequencies" in {
		val expectedTermFrequencies = TestData.termFrequenciesSet()
		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
			.filter(article => expectedTermFrequencies.exists(_._1 == article.title))
		val tfSet = articles
			.map(TermFrequencyCounter.extractBagOfWords(_, IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)))
			.collect
			.toSet
		tfSet shouldEqual expectedTermFrequencies.map(_._2)
	}

	"Wikipedia articles with contexts" should "be exactly these wikipedia articles" in {
		val expectedArticles = TestData.articlesWithContextSet()
		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)
		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
			.filter(article => expectedArticles.exists(_.title == article.title))
			.map(TermFrequencyCounter.extractArticleContext(_, tokenizer))
			.collect
			.toSet
		articles shouldEqual expectedArticles
	}

	"Link contexts" should "be exactly these bag of words" in {
		val oldSettings = TermFrequencyCounter.settings(false)
		TermFrequencyCounter.parseConfig()

		val linkContexts = TestData.parsedWikipediaWithTextsSet()
			.map(TermFrequencyCounter.extractLinkContexts(_, IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)))
			.flatMap(_.linkswithcontext)
		val expectedContexts = TestData.linksWithContextsSet()
		linkContexts shouldEqual expectedContexts

		TermFrequencyCounter.settings = oldSettings
	}

	they should "be extracted for both text and extendedlinks" in {
		val oldSettings = TermFrequencyCounter.settings
		TermFrequencyCounter.parseConfig()

		val linkContexts = TestData.parsedWikipediaExtendedLinksSet()
			.map(TermFrequencyCounter.extractLinkContexts(_, IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)))
			.flatMap(_.linkswithcontext)
		val expectedContexts = TestData.linksWithContextsSet()
		linkContexts shouldEqual expectedContexts

		TermFrequencyCounter.settings = oldSettings
	}

	"Articles with link context" should "contain exactly these link contexts" in {
		val oldSettings = TermFrequencyCounter.settings(false)
		TermFrequencyCounter.parseConfig()

		val expectedLinks = TestData.linksWithContextsSet()
		val enrichedLinks = TestData.parsedWikipediaWithTextsSet()
			.map(TermFrequencyCounter.extractLinkContexts(_, IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)))
			.flatMap(_.linkswithcontext)
		enrichedLinks shouldEqual expectedLinks

		TermFrequencyCounter.settings = oldSettings
	}

	they should "be exactly these articles" in {
		val oldSettings = TermFrequencyCounter.settings(false)
		TermFrequencyCounter.parseConfig()

		val enrichedLinkArticles = TestData.parsedWikipediaWithTextsSet()
			.map(TermFrequencyCounter.extractLinkContexts(_, IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)))
		val expectedArticles = TestData.articlesWithLinkContextsSet()
		enrichedLinkArticles shouldEqual expectedArticles

		TermFrequencyCounter.settings = oldSettings
	}

	"Articles without any context" should "be fully enriched with article and link contexts" in {
		val oldSettings = TermFrequencyCounter.settings(false)
		TermFrequencyCounter.parseConfig()


		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		val enrichedArticles = TermFrequencyCounter
			.run(List(articles).toAnyRDD(), sc, Array("CleanCoreNLPTokenizer", "true", "true"))
			.fromAnyRDD[ParsedWikipediaEntry]()
			.head
			.collect
			.toSet
		val expectedArticles = TestData.articlesWithCompleteContexts()
		enrichedArticles shouldEqual expectedArticles

		TermFrequencyCounter.settings = oldSettings
	}

	"Extracted link contexts" should "be exactly these contexts" in {
		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)
		val extractedLinks = TestData.linkContextExtractionData().map { article =>
			val tokens = tokenizer.onlyTokenizeWithOffset(article.getText())
			article.textlinks.map { link =>
				link.copy(context = TermFrequencyCounter.extractContext(tokens, link, tokenizer).getCounts())
			}
		}
		val expectedLinks = TestData.linkContextExtractionData().map(_.linkswithcontext)
		extractedLinks shouldEqual expectedLinks
	}
}
