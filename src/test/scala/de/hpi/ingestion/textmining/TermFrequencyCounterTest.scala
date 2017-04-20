package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class TermFrequencyCounterTest extends FlatSpec with SharedSparkContext with Matchers {

	"Term frequencies" should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val pagesTermFrequencies = articles
			.map(TermFrequencyCounter.extractBagOfWords)
		pagesTermFrequencies should not be empty
		pagesTermFrequencies
			.map(_.getCounts())
			.collect
			.foreach(bag => bag should not be empty)
	}

	they should "be exactly these term frequencies" in {
		val expectedTermFrequencies = TestData.termFrequenciesTestSet()
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.filter(article => expectedTermFrequencies.exists(_._1 == article.title))
		val tfSet = articles
			.map(TermFrequencyCounter.extractBagOfWords)
			.collect
			.toSet
		tfSet shouldEqual expectedTermFrequencies.map(_._2)
	}

	"Wikipedia articles with contexts" should "be exactly these wikipedia articles" in {
		val expectedArticles = TestData.articlesWithContextTestSet()
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.filter(article => expectedArticles.exists(_.title == article.title))
			.map(TermFrequencyCounter.extractArticleContext)
			.collect
			.toSet
		articles shouldEqual expectedArticles
	}

	"Link contexts" should "be exactly these bag of words" in {
		val linkContexts = TestData.parsedWikipediaTestSet()
			.map(TermFrequencyCounter.extractLinkContexts)
		    .flatMap(_.linkswithcontext)
		val expectedContexts = TestData.linksWithContextsTestSet()
		linkContexts shouldEqual expectedContexts
	}

	"Articles with link context" should "contain exactly these link contexts" in {
		val expectedLinks = TestData.linksWithContextsTestSet()
		val enrichedLinks = TestData.parsedWikipediaTestSet()
			.map(TermFrequencyCounter.extractLinkContexts)
			.flatMap(_.linkswithcontext)
		enrichedLinks shouldEqual expectedLinks
	}

	they should "be exactly these articles" in {
		val enrichedLinkArticles = TestData.parsedWikipediaTestSet()
			.map(TermFrequencyCounter.extractLinkContexts)
		val expectedArticles = TestData.articlesWithLinkContextsTestSet()
		enrichedLinkArticles shouldEqual expectedArticles
	}

	"Articles without any context" should "be fully enriched with article and link contexts" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val enrichedArticles = TermFrequencyCounter.run(articles)
			.collect
			.toSet
		val expectedArticles = TestData.articlesWithCompleteContexts()
		enrichedArticles shouldEqual expectedArticles
	}
}