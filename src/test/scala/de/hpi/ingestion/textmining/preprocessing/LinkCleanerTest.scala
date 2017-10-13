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

package de.hpi.ingestion.textmining.preprocessing

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.textmining.TestData
import de.hpi.ingestion.textmining.models.ParsedWikipediaEntry
import org.scalatest.{FlatSpec, Matchers}

class LinkCleanerTest extends FlatSpec with SharedSparkContext with Matchers {
	"Grouped valid pages with known existing pages" should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaSetWithoutText().toList)
		val pages = sc.parallelize(TestData.cleanedGroupedPagesSet().toList)
			.map(_.page)
		val validPages = LinkCleaner.groupValidPages(articles, pages)
		validPages should not be empty
	}

	they should "be exactly these pages" in {
		val articles = sc.parallelize(TestData.parsedWikipediaSetWithoutText().toList)
		val pages = sc.parallelize(TestData.cleanedGroupedPagesSet().toList)
			.map(_.page)
		val validPages = LinkCleaner.groupValidPages(articles, pages)
			.collect
			.toSet
		validPages shouldBe TestData.groupedValidPagesSet()
	}

	"Grouped valid pages without known existing pages" should "be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaSetWithoutText().toList)
		val pages = articles.map(_.title)
		val validPages = LinkCleaner.groupValidPages(articles, pages)
			.collect
			.toSet
		validPages shouldBe empty
	}

	"Cleaned Wikipedia articles with known valid pages" should "be exactly these articles" in {
		val articles = sc.parallelize(TestData.parsedWikipediaSetWithoutText().toList)
		val validPages = sc.parallelize(TestData.groupedValidPagesSet().toList)
		val cleanedArticles = LinkCleaner.filterArticleLinks(articles, validPages)
			.collect
			.toSet
		cleanedArticles shouldEqual TestData.cleanedParsedWikipediaSet()
	}

	"Cleaned Wikipedia articles without valid pages" should "not contain links" in {
		val articles = sc.parallelize(TestData.parsedWikipediaSetWithoutText().toList)
		val validPages = sc.parallelize(List[(String, Set[String])]())
		LinkCleaner.filterArticleLinks(articles, validPages)
			.collect
			.foreach(_.allLinks() shouldBe empty)
	}

	"Wikipedia articles without links" should "be removed" in {
		val job = new LinkCleaner
		job.parsedWikipedia = sc.parallelize(TestData.parsedArticlesWithoutLinksSet().toList)
		job.run(sc)
		job.cleanedParsedWikipedia shouldBe empty
	}

	"Cleaned Wikipedia articles from incomplete article set" should "be empty" in {
		val job = new LinkCleaner
		job.parsedWikipedia = sc.parallelize(TestData.parsedWikipediaSetWithoutText().toList)
		job.run(sc)
		job.cleanedParsedWikipedia shouldBe empty
	}

	"Cleaned Wikipedia articles" should "be exactly these articles" in {
		val job = new LinkCleaner
		job.parsedWikipedia = sc.parallelize(TestData.closedParsedWikipediaSet().toList)
		job.run(sc)
		val cleanedArticles = job
			.cleanedParsedWikipedia
			.collect
			.toSet
		cleanedArticles shouldEqual TestData.cleanedClosedParsedWikipediaSet()
	}
}
