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

package de.hpi.ingestion.textmining.nel

import de.hpi.ingestion.textmining.models.{Link, WikipediaArticleCount}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import de.hpi.ingestion.textmining.{TestData => TextTestData}
import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import de.hpi.ingestion.textmining.preprocessing.CosineContextComparator

class TextNELTest extends FlatSpec with Matchers with SharedSparkContext {

	"Entities" should "be tagged in articles" in {
		val oldModelFunction = TextNEL.loadModelFunction
		val oldAliasPageScores = TextNEL.aliasPageScores
		val oldDocFreqFunction = CosineContextComparator.docFreqStreamFunction

		val session = SparkSession.builder().getOrCreate()
		TextNEL.aliasPageScores = Map()
		TextNEL.loadModelFunction = TestData.randomForestModel(session)
		val testDocFreqFunction = TextTestData.docfreqStream("docfreq") _
		CosineContextComparator.docFreqStreamFunction = testDocFreqFunction

		val articles = sc.parallelize(TestData.foundAliasArticles())
		val wikipediaTfIdf = sc.parallelize(TestData.tfidfArticles())
		val numDocuments = sc.parallelize(List(WikipediaArticleCount("parsedwikipedia", wikipediaTfIdf.count())))
		val aliases = sc.parallelize(TestData.rawAliases())
		val input = List(articles, numDocuments, aliases, wikipediaTfIdf)
			.flatMap(List(_).toAnyRDD())
		val linkedEntities = TextNEL.run(input, sc)
			.fromAnyRDD[(String, List[Link])]()
			.head
			.collect
			.toSet
		val expectedEntities = TestData.linkedEntitiesForAllArticles()
		linkedEntities shouldEqual expectedEntities

		TextNEL.loadModelFunction = oldModelFunction
		TextNEL.aliasPageScores = oldAliasPageScores
		CosineContextComparator.docFreqStreamFunction = oldDocFreqFunction
	}

	"Found named entities for incomplete articles" should "be exactly these named entities" in {
		val oldModelFunction = TextNEL.loadModelFunction
		val oldAliasPageScores = TextNEL.aliasPageScores
		val oldDocFreqFunction = CosineContextComparator.docFreqStreamFunction

		val session = SparkSession.builder().getOrCreate()
		TextNEL.loadModelFunction = TestData.randomForestModel(session)
		val testDocFreqFunction = TextTestData.docfreqStream("docfreq") _
		TextNEL.aliasPageScores = Map()
		CosineContextComparator.docFreqStreamFunction = testDocFreqFunction

		val articles = sc.parallelize(TestData.incompleteFoundAliasArticles())
		val wikipediaTfidf = sc.parallelize(TestData.contextArticles())
		val concatenatedAliases = TextTestData.aliasesWithExistingPagesSet() + TestData.alias
		val aliases = sc.parallelize(concatenatedAliases.toList)
		val numDocuments = sc.parallelize(List(WikipediaArticleCount("parsedwikipedia", wikipediaTfidf.count())))
		val input = List(articles, numDocuments, aliases, wikipediaTfidf)
			.flatMap(List(_).toAnyRDD())
		val linkedEntities = TextNEL.run(input, sc)
			.fromAnyRDD[(String, List[Link])]()
			.head
			.map(articleLinks => (articleLinks._1, articleLinks._2.toSet))
			.collect
			.toSet
		val expectedEntities = TestData.linkedEntitiesForIncompleteArticles()
		linkedEntities shouldEqual expectedEntities

		TextNEL.loadModelFunction = oldModelFunction
		TextNEL.aliasPageScores = oldAliasPageScores
		CosineContextComparator.docFreqStreamFunction = oldDocFreqFunction
	}

	"Alias page scores" should "not be recalculated" in {
		val oldModelFunction = TextNEL.loadModelFunction
		val oldDocFreqFunction = CosineContextComparator.docFreqStreamFunction
		val oldAliases = TextNEL.aliasPageScores

		val session = SparkSession.builder().getOrCreate()
		TextNEL.loadModelFunction = TestData.randomForestModel(session)
		val testDocFreqFunction = TextTestData.docfreqStream("docfreq") _
		CosineContextComparator.docFreqStreamFunction = testDocFreqFunction
		TextNEL.aliasPageScores = TestData.aliasMap()

		val articles = sc.parallelize(TestData.foundAliasArticles())
		val numDocuments = sc.parallelize(List(WikipediaArticleCount("parsedwikipedia", 3)))
		val aliases = sc.parallelize(Nil)
		val wikipediaTfIdf = sc.parallelize(TestData.tfidfArticles())
		val input = List(articles, numDocuments, aliases, wikipediaTfIdf).flatMap(List(_).toAnyRDD())
		val linkedEntities = TextNEL.run(input, sc).fromAnyRDD[(String, List[Link])]().head.collect.toSet
		val expectedEntities = TestData.linkedEntitiesForAllArticles()
		linkedEntities shouldEqual expectedEntities

		CosineContextComparator.docFreqStreamFunction = oldDocFreqFunction
		TextNEL.loadModelFunction = oldModelFunction
		TextNEL.aliasPageScores = oldAliases
	}

	"Extracted TrieAlias contexts" should "be exactly these contexts" in {
		val enrichedAliases = TestData.foundAliasArticles()
			.flatMap(TextNEL.extractTrieAliasContexts(_, IngestionTokenizer(true, true)))
		val expectedAliases = TestData.aliasContexts()
		enrichedAliases shouldEqual expectedAliases
	}

	"Classified Feature Entries" should "be exactly these entries" in {
		val session = SparkSession.builder().getOrCreate()
		val featureEntries = sc.parallelize(TestData.featureEntries())
		val model = TestData.randomForestModel(session)()
		val linkedEntities = TextNEL.classifyFeatureEntries(featureEntries, model, session).collect.toSet
		val expectedEntities = TestData.linkedEntities()
		linkedEntities shouldEqual expectedEntities
	}

	"Input articles" should "be split" in {
		val oldSettings = TextNEL.settings(false)

		val articles = sc.parallelize(TestData.foundAliasArticles())
		val numDocuments = sc.parallelize(List(WikipediaArticleCount("parsedwikipedia", 3)))
		val aliases = sc.parallelize(TestData.rawAliases())
		val wikipediaTfIdf = sc.parallelize(TestData.tfidfArticles())
		val input = List(articles, numDocuments, aliases, wikipediaTfIdf).flatMap(List(_).toAnyRDD())

		TextNEL.settings = Map("NELTable" -> "wikipedianel")
		val splitInputWikipedia = TextNEL.splitInput(input)
		splitInputWikipedia should have size 100
		TextNEL.settings = Map("NELTable" -> "spiegel")
		val splitInputSpiegel = TextNEL.splitInput(input)
		splitInputSpiegel should have size 20

		TextNEL.settings = oldSettings
	}
}
