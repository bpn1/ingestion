package de.hpi.ingestion.textmining.nel

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.textmining.models.{Link, WikipediaArticleCount}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import de.hpi.ingestion.textmining.{AliasTrieSearch, CosineContextComparator, TestData => TextTestData}
import org.scalatest.{FlatSpec, Matchers}

class TextNELTest extends FlatSpec with Matchers with SharedSparkContext {

	"Entities" should "be tagged in articles" in {
		val oldModelFunction = TextNEL.loadModelFunction
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldSettings = CosineContextComparator.settings(false)
		CosineContextComparator.parseConfig()

		val testModelFunction = TextTestData.hdfsRandomForestModel(1.0) _
		TextNEL.loadModelFunction = testModelFunction
		val testDocFreqFunction = TextTestData.docfreqStream("docfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val articles = sc.parallelize(TestData.foundAliasArticles())
		val numDocuments = sc.parallelize(List(WikipediaArticleCount("parsedwikipedia", 3)))
		val aliases = sc.parallelize(TestData.rawAliases())
		val wikipediaTfIdf = sc.parallelize(TestData.tfidfArticles())
		val input = List(articles, numDocuments, aliases, wikipediaTfIdf).flatMap(List(_).toAnyRDD())
		val linkedEntities = TextNEL.run(input, sc).fromAnyRDD[(String, List[Link])]().head.collect.toSet
		val expectedEntties = TestData.linkedEntities()
		linkedEntities shouldEqual expectedEntties

		CosineContextComparator.settings = oldSettings
		TextNEL.loadModelFunction = oldModelFunction
		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	"Alias page scores" should "not be recalculated" in {
		val oldModelFunction = TextNEL.loadModelFunction
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldSettings = CosineContextComparator.settings(false)
		CosineContextComparator.parseConfig()
		val oldAliases = TextNEL.aliasPageScores

		val testModelFunction = TextTestData.hdfsRandomForestModel(1.0) _
		TextNEL.loadModelFunction = testModelFunction
		val testDocFreqFunction = TextTestData.docfreqStream("docfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		TextNEL.aliasPageScores = TestData.aliasMap()
		val articles = sc.parallelize(TestData.foundAliasArticles())
		val numDocuments = sc.parallelize(List(WikipediaArticleCount("parsedwikipedia", 3)))
		val aliases = sc.parallelize(Nil)
		val wikipediaTfIdf = sc.parallelize(TestData.tfidfArticles())
		val input = List(articles, numDocuments, aliases, wikipediaTfIdf).flatMap(List(_).toAnyRDD())
		val linkedEntities = TextNEL.run(input, sc).fromAnyRDD[(String, List[Link])]().head.collect.toSet
		val expectedEntties = TestData.linkedEntities()
		linkedEntities shouldEqual expectedEntties

		CosineContextComparator.settings = oldSettings
		TextNEL.loadModelFunction = oldModelFunction
		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
		TextNEL.aliasPageScores = oldAliases
	}

	"Extracted TrieAlias contexts" should "be exactly these contexts" in {
		val enrichedAliases = TestData.foundAliasArticles()
		    .flatMap(TextNEL.extractTrieAliasContexts(_, IngestionTokenizer(true, true)))
		val expectedAliases = TestData.aliasContexts()
		enrichedAliases shouldEqual expectedAliases
	}

	"Classified Feature Entries" should "be exactly these entries" in {
		val featureEntries = sc.parallelize(TestData.featureEntries())
		val model = sc.broadcast(TextTestData.randomForestModel(1.0))
		val linkedEntities = TextNEL.classifyFeatureEntries(featureEntries, model).collect.toSet
		val expectedEntties = TestData.linkedEntities()
		linkedEntities shouldEqual expectedEntties
	}

	"Input articles" should "be split" in {
		val articles = sc.parallelize(TestData.foundAliasArticles())
		val numDocuments = sc.parallelize(List(WikipediaArticleCount("parsedwikipedia", 3)))
		val aliases = sc.parallelize(TestData.rawAliases())
		val wikipediaTfIdf = sc.parallelize(TestData.tfidfArticles())
		val input = List(articles, numDocuments, aliases, wikipediaTfIdf).flatMap(List(_).toAnyRDD())
		val splitInput = TextNEL.splitInput(input)
		splitInput should have size 20
	}
}
