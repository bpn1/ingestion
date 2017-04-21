package de.hpi.ingestion.textmining

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext

class LinkCleanerTest extends FlatSpec with SharedSparkContext with Matchers {
	"Grouped valid pages with known existing pages" should "not be empty" in {
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
		validPages shouldBe TestData.groupedValidPagesSet()
	}

	"Grouped valid pages without known existing pages" should "be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaSet().toList)
		val pages = articles.map(_.title)
		val validPages = LinkCleaner.groupValidPages(articles, pages)
			.collect
			.toSet
		validPages shouldBe empty
	}

	"Cleaned Wikipedia articles with known valid pages" should "be exactly these articles" in {
		val articles = sc.parallelize(TestData.parsedWikipediaSet().toList)
		val validPages = sc.parallelize(TestData.groupedValidPagesSet().toList)
		val cleanedArticles = LinkCleaner.filterArticleLinks(articles, validPages)
			.collect
			.toSet
		cleanedArticles shouldEqual TestData.cleanedParsedWikipediaSet()
	}

	"Cleaned Wikipedia articles without valid pages" should "not contain links" in {
		val articles = sc.parallelize(TestData.parsedWikipediaSet().toList)
		val validPages = sc.parallelize(List[(String, Set[String])]())
		LinkCleaner.filterArticleLinks(articles, validPages)
			.collect
			.foreach(_.allLinks() shouldBe empty)
	}

	"Wikipedia articles without links" should "be removed" in {
		val articles = sc.parallelize(TestData.parsedArticlesWithoutLinksSet().toList)
		val cleanedArticles = LinkCleaner.run(articles)
		cleanedArticles shouldBe empty
	}

	"Cleaned Wikipedia articles from incomplete article set" should "be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaSet().toList)
		val cleanedArticles = LinkCleaner.run(articles)
		cleanedArticles shouldBe empty
	}

	they should "not contain links" in {
		val articles = sc.parallelize(TestData.parsedWikipediaSet().toList)
		LinkCleaner.run(articles)
			.collect
			.foreach(_.allLinks() shouldBe empty)
	}

	"Cleaned Wikipedia articles" should "be exactly these articles" in {
		val articles = sc.parallelize(TestData.closedParsedWikipediaSet().toList)
		val cleanedArticles = LinkCleaner.run(articles)
			.collect
			.toSet
		cleanedArticles shouldEqual TestData.cleanedClosedParsedWikipediaSet()
	}
}
