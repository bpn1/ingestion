package de.hpi.ingestion.textmining

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext

class LinkCleanerTest extends FlatSpec with SharedSparkContext with Matchers {
	"Grouped valid pages" should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaSet().toList)
		val pages = sc.parallelize(TestData.cleanedGroupedPagesTestSet().toList)
			.map(_.page)
		val validPages = LinkCleaner.groupValidPages(articles, pages)
		validPages should not be empty
	}

	they should "be exactly these pages" in {
		val articles = sc.parallelize(TestData.parsedWikipediaSet().toList)
		val pages = sc.parallelize(TestData.cleanedGroupedPagesTestSet().toList)
			.map(_.page)
		val validPages = LinkCleaner.groupValidPages(articles, pages)
			.collect
			.toSet
		validPages shouldBe TestData.groupedValiPagesSet()
	}

	"Cleaned Wikipedia articles with known valid pages" should "be exactly these articles" in {
		val articles = sc.parallelize(TestData.parsedWikipediaSet().toList)
		val validPages = sc.parallelize(TestData.groupedValiPagesSet().toList)
		val cleanedArticles = LinkCleaner.filterArticleLinks(articles, validPages)
			.collect
			.toSet
		cleanedArticles shouldEqual TestData.cleanedParsedWikipediaSet()
	}

	"Wikipedia articles without links" should "not be removed" in {
		val articles = sc.parallelize(TestData.parsedArticlesWithoutLinksSet().toList)
		val pages = sc.parallelize(TestData.cleanedGroupedPagesTestSet().toList)
			.map(_.page)
		val cleanedArticles = LinkCleaner.run(articles, pages)
		cleanedArticles should not be empty
	}

	"Cleaned Wikipedia articles" should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaSet().toList)
		val pages = sc.parallelize(TestData.cleanedGroupedPagesTestSet().toList)
			.map(_.page)
		val cleanedArticles = LinkCleaner.run(articles, pages)
		cleanedArticles should not be empty
	}

	they should "be exactly these articles" in {
		val articles = sc.parallelize(TestData.parsedWikipediaSet().toList)
		val pages = sc.parallelize(TestData.cleanedGroupedPagesTestSet().toList)
			.map(_.page)
		val cleanedArticles = LinkCleaner.run(articles, pages)
			.collect
			.toSet
		cleanedArticles shouldEqual TestData.cleanedParsedWikipediaSet()
	}
}
