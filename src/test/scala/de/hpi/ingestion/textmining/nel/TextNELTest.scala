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
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import de.hpi.ingestion.textmining.{TestData => TextTestData}
import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import de.hpi.ingestion.textmining.preprocessing.CosineContextComparator

class TextNELTest extends FlatSpec with Matchers with SharedSparkContext {

    "Entities" should "be tagged in articles" in {
        val job = new TextNEL
        val session = SparkSession.builder().getOrCreate()
        job.aliasPageScores = Map()
        job.loadModelFunction = TestData.randomForestModel(session)
        job.docFreqStreamFunction = TextTestData.docfreqStream("docfreq")
        job.trieArticles = sc.parallelize(TestData.foundAliasArticles())
        job.articleTfidf = sc.parallelize(TestData.tfidfArticles())
        job.numDocuments = job.articleTfidf.count
        job.aliases = sc.parallelize(TestData.rawAliases())
        job.run(sc)
        val linkedEntities = job.entityLinks
            .reduce(_.union(_))
            .collect
            .toSet
        val expectedEntities = TestData.linkedEntitiesForAllArticles()
        linkedEntities shouldEqual expectedEntities
    }

    "Found named entities for incomplete articles" should "be exactly these named entities" in {
        val job = new TextNEL
        val session = SparkSession.builder().getOrCreate()
        job.loadModelFunction = TestData.randomForestModel(session)
        job.aliasPageScores = Map()
        job.docFreqStreamFunction = TextTestData.docfreqStream("docfreq")

        job.trieArticles = sc.parallelize(TestData.incompleteFoundAliasArticles())
        job.articleTfidf = sc.parallelize(TestData.contextArticles())
        val concatenatedAliases = TextTestData.aliasesWithExistingPagesSet() + TestData.alias
        job.aliases = sc.parallelize(concatenatedAliases.toList)
        job.numDocuments = job.articleTfidf.count()
        job.run(sc)
        val linkedEntities = job.entityLinks
            .reduce(_.union(_))
            .map(articleLinks => (articleLinks._1, articleLinks._2.toSet))
            .collect
            .toSet
        val expectedEntities = TestData.linkedEntitiesForIncompleteArticles()
        linkedEntities shouldEqual expectedEntities
    }

    "Alias page scores" should "not be recalculated" in {
        val job = new TextNEL
        val session = SparkSession.builder().getOrCreate()
        job.loadModelFunction = TestData.randomForestModel(session)
        job.docFreqStreamFunction = TextTestData.docfreqStream("docfreq")
        job.aliasPageScores = TestData.aliasMap()

        job.trieArticles = sc.parallelize(TestData.foundAliasArticles())
        job.numDocuments = 3
        job.aliases = sc.parallelize(Nil)
        job.articleTfidf = sc.parallelize(TestData.tfidfArticles())
        job.run(sc)
        val linkedEntities = job.entityLinks
            .reduce(_.union(_))
            .collect
            .toSet
        val expectedEntities = TestData.linkedEntitiesForAllArticles()
        linkedEntities shouldEqual expectedEntities
    }

    "Extracted TrieAlias contexts" should "be exactly these contexts" in {
        val job = new TextNEL
        val contextSize = job.settings("contextSize").toInt
        val enrichedAliases = TestData.foundAliasArticles()
            .flatMap(TextNEL.extractTrieAliasContexts(_, IngestionTokenizer(true, true), contextSize))
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
        val job = new TextNEL
        job.trieArticles = sc.parallelize(TestData.foundAliasArticles())
        job.settings = Map("NELTable" -> "wikipedianel")
        val splitInputWikipedia = job.splitInput()
        splitInputWikipedia should have size 100
        job.settings = Map("NELTable" -> "spiegel")
        val splitInputSpiegel = job.splitInput()
        splitInputSpiegel should have size 20
    }
}
