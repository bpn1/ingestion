package de.hpi.ingestion.textmining

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext

class LinkCleanerTest extends FlatSpec with SharedSparkContext with Matchers {
	"Grouped valid pages" should "not be empty" in {
		val articles = sc.parallelize(TestData.smallerParsedWikipediaList())
		val pages = sc.parallelize(TestData.cleanedGroupedPagesTestSet().toList)
			.map(_.page)
		val validPages = LinkCleaner.groupValidPages(articles, pages)
			.collect
		validPages should not be empty
	}

	they should "be exactly these pages" in {
		val articles = sc.parallelize(TestData.smallerParsedWikipediaList())
		val pages = sc.parallelize(TestData.cleanedGroupedPagesTestSet().toList)
			.map(_.page)
		val validPages = LinkCleaner.groupValidPages(articles, pages)
			.collect
			.toSet
		validPages shouldBe TestData.groupedValiPagesSet()
	}

	"Cleaned Wikipedia articles with known valid pages" should "be exactly these articles" in {
		val articles = sc.parallelize(TestData.smallerParsedWikipediaList())
		val validPages = sc.parallelize(TestData.groupedValiPagesSet().toList)
		val cleanedArticles = LinkCleaner.filterArticleLinks(articles, validPages)
			.collect
			.toList
		cleanedArticles shouldEqual TestData.cleanedSmallerParsedWikipediaList()
	}

	"Cleaned Wikipedia articles" should "not be empty" in {
		val articles = sc.parallelize(TestData.smallerParsedWikipediaList())
		val pages = sc.parallelize(TestData.cleanedGroupedPagesTestSet().toList)
			.map(_.page)
		val cleanedArticles = LinkCleaner.run(articles, pages)
			.collect
		cleanedArticles should not be empty
	}

	they should "be exactly these articles" in {
		val articles = sc.parallelize(TestData.smallerParsedWikipediaList())
		val pages = sc.parallelize(TestData.cleanedGroupedPagesTestSet().toList)
			.map(_.page)
		val cleanedArticles = LinkCleaner.run(articles, pages)
			.collect
			.toList
		cleanedArticles shouldEqual TestData.cleanedSmallerParsedWikipediaList()
	}
}
