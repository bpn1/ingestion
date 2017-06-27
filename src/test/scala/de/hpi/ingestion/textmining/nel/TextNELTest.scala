//package de.hpi.ingestion.textmining.nel
//
//import de.hpi.ingestion.implicits.CollectionImplicits._
//import com.holdenkarau.spark.testing.SharedSparkContext
//import org.scalatest.{FlatSpec, Matchers}
//
//class TextNELTest extends FlatSpec with SharedSparkContext with Matchers {
//	"Found named entities" should "not be empty" in {
//		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
//		val testTrieStreamFunction = TestData.fullTrieStream _
//		AliasTrieSearch.trieStreamFunction = testTrieStreamFunction
//		val oldDocFreqStreamFunction = CosineContextComparator.docfreqStreamFunction
//		val testDocFreqStreamFunction = TestData.docfreqStream("docfreq") _
//		CosineContextComparator.docfreqStreamFunction = testDocFreqStreamFunction
//		val oldModelFunction = TextNEL.loadModelFunction
//		val testModelFunction = TestData.randomForestModelWithPositivePrediciton _
//		TextNEL.loadModelFunction = testModelFunction
//
//		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
//		val aliases = sc.parallelize(TestData.aliasesWithExistingPagesSet().toList)
//		val input = List(articles).toAnyRDD() ++ List(aliases).toAnyRDD() ++ List(articles).toAnyRDD()
//		val namedEntities = TextNEL.run(input, sc)
//			.fromAnyRDD[Link]()
//			.head
//			.collect
//		namedEntities should not be empty
//
//		TextNEL.loadModelFunction = oldModelFunction
//		CosineContextComparator.docfreqStreamFunction = oldDocFreqStreamFunction
//		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
//	}
//
//	they should "be exactly these named entities" in {
//		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
//		val testTrieStreamFunction = TestData.fullTrieStream _
//		AliasTrieSearch.trieStreamFunction = testTrieStreamFunction
//		val oldDocFreqStreamFunction = CosineContextComparator.docfreqStreamFunction
//		val testDocFreqStreamFunction = TestData.docfreqStream("docfreq") _
//		CosineContextComparator.docfreqStreamFunction = testDocFreqStreamFunction
//		val oldModelFunction = TextNEL.loadModelFunction
//		val testModelFunction = TestData.randomForestModelWithPositivePrediciton _
//		TextNEL.loadModelFunction = testModelFunction
//
//		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
//		val aliases = sc.parallelize(TestData.aliasesWithExistingPagesSet().toList)
//		val input = List(articles).toAnyRDD() ++ List(aliases).toAnyRDD() ++ List(articles).toAnyRDD()
//		val namedEntities = TextNEL.run(input, sc)
//			.fromAnyRDD[Link]()
//			.head
//			.collect
//			.toSet
//		namedEntities shouldEqual TestData.namedEntitiesSet()
//
//		TextNEL.loadModelFunction = oldModelFunction
//		CosineContextComparator.docfreqStreamFunction = oldDocFreqStreamFunction
//		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
//	}
//
//	"Found named entities for incomplete articles" should "be exactly these named entities" in {
//		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
//		val testTrieStreamFunction = TestData.fullTrieStream _
//		AliasTrieSearch.trieStreamFunction = testTrieStreamFunction
//		val oldDocFreqStreamFunction = CosineContextComparator.docfreqStreamFunction
//		val testDocFreqStreamFunction = TestData.docfreqStream("docfreq") _
//		CosineContextComparator.docfreqStreamFunction = testDocFreqStreamFunction
//		val oldModelFunction = TextNEL.loadModelFunction
//		val testModelFunction = TestData.randomForestModelWithPositivePrediciton _
//		TextNEL.loadModelFunction = testModelFunction
//
//		val concatenatedArticles = TestData.articlesWithCompleteContexts() ++ TestData.articlesWithoutContexts() +
//			TestData.articleWithCompleteContext()
//		val concatenatedIncompleteArticles = TestData.incompleteArticlesWithCompleteContexts() +
//			TestData.articleWithCompleteContext
//		val concatenatedAliases = TestData.aliasesWithExistingPagesSet() + TestData.alias
//		val articles = sc.parallelize(concatenatedArticles.toList)
//		val incompleteArticles = sc.parallelize(concatenatedIncompleteArticles.toList)
//		val aliases = sc.parallelize(concatenatedAliases.toList)
//		val input = List(articles).toAnyRDD() ++ List(aliases).toAnyRDD() ++ List(incompleteArticles).toAnyRDD()
//		val namedEntities = TextNEL.run(input, sc)
//			.fromAnyRDD[Link]()
//			.head
//			.collect
//			.toSet
//		namedEntities shouldEqual TestData.namedEntitiesForIncompleteArticlesSet()
//
//		TextNEL.loadModelFunction = oldModelFunction
//		CosineContextComparator.docfreqStreamFunction = oldDocFreqStreamFunction
//		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
//	}
//
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
//		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
//		val aliases = sc.parallelize(TestData.aliasesWithExistingPagesSet().toList)
//		val input = List(articles).toAnyRDD() ++ List(aliases).toAnyRDD()
//		val splitInput = TextNEL.splitInput(input).toList
//		splitInput should have length 10
//		splitInput.foreach { case inputPartition =>
//			inputPartition should have length 3
//		}
//	}
//}

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
