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
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.TestData
import de.hpi.ingestion.textmining.models.ParsedWikipediaEntry
import de.hpi.ingestion.textmining.tokenizer.{CleanCoreNLPTokenizer, IngestionTokenizer}
import org.scalatest.{FlatSpec, Matchers}

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
		val linkContexts = TestData.parsedWikipediaWithTextsSet()
			.map(TermFrequencyCounter.extractLinkContexts(_, IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)))
			.flatMap(_.linkswithcontext)
		val expectedContexts = TestData.linksWithContextsSet()
		linkContexts shouldEqual expectedContexts
	}

	they should "be extracted for both text and extendedlinks" in {
		val linkContexts = TestData.parsedWikipediaExtendedLinksSet()
			.map(TermFrequencyCounter.extractLinkContexts(_, IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)))
			.flatMap(_.linkswithcontext)
		val expectedContexts = TestData.linksWithContextsSet()
		linkContexts shouldEqual expectedContexts
	}

	"Articles with link context" should "contain exactly these link contexts" in {
		val expectedLinks = TestData.linksWithContextsSet()
		val enrichedLinks = TestData.parsedWikipediaWithTextsSet()
			.map(TermFrequencyCounter.extractLinkContexts(_, IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)))
			.flatMap(_.linkswithcontext)
		enrichedLinks shouldEqual expectedLinks
	}

	they should "be exactly these articles" in {
		val enrichedLinkArticles = TestData.parsedWikipediaWithTextsSet()
			.map(TermFrequencyCounter.extractLinkContexts(_, IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)))
		val expectedArticles = TestData.articlesWithLinkContextsSet()
		enrichedLinkArticles shouldEqual expectedArticles
	}

	"Articles without any context" should "be fully enriched with article and link contexts" in {
		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		val enrichedArticles = TermFrequencyCounter
			.run(List(articles).toAnyRDD(), sc, Array("CleanCoreNLPTokenizer", "true", "true"))
			.fromAnyRDD[ParsedWikipediaEntry]()
			.head
			.collect
			.toSet
		val expectedArticles = TestData.articlesWithCompleteContexts()
		enrichedArticles shouldEqual expectedArticles
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
