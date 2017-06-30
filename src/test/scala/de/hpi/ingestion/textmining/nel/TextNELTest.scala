package de.hpi.ingestion.textmining.nel

import de.hpi.ingestion.textmining.models.{Link, WikipediaArticleCount}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import de.hpi.ingestion.textmining.{CosineContextComparator, TestData => TextTestData}
import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext

class TextNELTest extends FlatSpec with Matchers with SharedSparkContext {
	"Entities" should "be tagged in articles" in {
		val oldModelFunction = TextNEL.loadModelFunction
		val testModelFunction = TextTestData.hdfsRandomForestModel(1.0) _
		TextNEL.loadModelFunction = testModelFunction
		val testDocFreqFunction = TextTestData.docfreqStream("docfreq") _
		val oldAliasPageScores = TextNEL.aliasPageScores
		TextNEL.aliasPageScores = Map()
		val oldSettings = CosineContextComparator.settings(false)
		val oldDocFreqFunction = CosineContextComparator.docFreqStreamFunction
		CosineContextComparator.parseConfig()
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
		val expectedEntities = TestData.linkedEntities()
		linkedEntities shouldEqual expectedEntities

		TextNEL.loadModelFunction = oldModelFunction
		TextNEL.aliasPageScores = oldAliasPageScores
		CosineContextComparator.settings = oldSettings
		CosineContextComparator.docFreqStreamFunction = oldDocFreqFunction
	}

	"Found named entities for incomplete articles" should "be exactly these named entities" in {
		val oldModelFunction = TextNEL.loadModelFunction
		val testModelFunction = TextTestData.hdfsRandomForestModel(1.0) _
		TextNEL.loadModelFunction = testModelFunction
		val testDocFreqFunction = TextTestData.docfreqStream("docfreq") _
		val oldAliasPageScores = TextNEL.aliasPageScores
		TextNEL.aliasPageScores = Map()
		val oldSettings = CosineContextComparator.settings(false)
		val oldDocFreqFunction = CosineContextComparator.docFreqStreamFunction
		CosineContextComparator.parseConfig()
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
			.collect
			.toSet
		val expectedEntities = TestData.linkedEntitiesForIncompleteArticles()
		linkedEntities shouldEqual expectedEntities

		TextNEL.loadModelFunction = oldModelFunction
		TextNEL.aliasPageScores = oldAliasPageScores
		CosineContextComparator.settings = oldSettings
		CosineContextComparator.docFreqStreamFunction = oldDocFreqFunction
	}

	"Alias page scores" should "not be recalculated" in {
		val oldModelFunction = TextNEL.loadModelFunction
		val oldDocFreqFunction = CosineContextComparator.docFreqStreamFunction
		val oldSettings = CosineContextComparator.settings(false)
		CosineContextComparator.parseConfig()
		val oldAliases = TextNEL.aliasPageScores

		val testModelFunction = TextTestData.hdfsRandomForestModel(1.0) _
		TextNEL.loadModelFunction = testModelFunction
		val testDocFreqFunction = TextTestData.docfreqStream("docfreq") _
		CosineContextComparator.docFreqStreamFunction = testDocFreqFunction
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
		CosineContextComparator.docFreqStreamFunction = oldDocFreqFunction
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

	//	"Alias matches" should "be found" in {
	//		val tokenizer = IngestionTokenizer(false, false)
	//		val contextTokenizer = IngestionTokenizer(true, true)
	//		val trie = TestData.dataTrie(tokenizer)
	//		val texts = TestData.parsedWikipediaWithTextsSet().map(_.getText())
	//		texts
	//			.map(TextNEL.matchTrieAliases(_, trie, tokenizer, contextTokenizer))
	//			.foreach(_ should not be empty)
	//	}
	//
	//	"Input split" should "split input into 10 parts with each one consisting of 3 parts" in {
	//		val articles = sc.parallelize(TestData.contextArticles().toList)
	//		val aliases = sc.parallelize(TestData.aliasesWithExistingPagesSet().toList)
	//		val input = List(articles).toAnyRDD() ++ List(aliases).toAnyRDD()
	//		val splitInput = TextNEL.splitInput(input).toList
	//		splitInput should have length 10
	//		splitInput.foreach { case inputPartition =>
	//			inputPartition should have length 3
	//		}
	//	}
}
