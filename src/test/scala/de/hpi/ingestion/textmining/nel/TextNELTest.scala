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
//	they should "contain html links" in {
//		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
//			.map(article => Article(article.title, article.getText()))
//		val links = sc.parallelize(TestData.linksWithArticleAnnotation().toList)
//		val anchorRegex = new Regex("(?s)<a href=\".+?\">.+?<\\/a>")
//		TextNEL.generateArticlesWithLinks(articles, links)
//			.collect
//			.foreach { article =>
//				if(article.title != "Audi Test ohne Link") {
//					anchorRegex.findFirstIn(article.text) should not be empty
//				}
//			}
//	}
//
//	they should "only contain the title and the original text" in {
//		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
//			.map(article => Article(article.title, article.getText()))
//		val links = sc.parallelize(TestData.linksWithArticleAnnotation().toList)
//		val originalTexts = articles
//			.map(article => article.title + "\n" + article.text)
//			.collect
//			.toSet
//		val texts = TextNEL.generateArticlesWithLinks(articles, links)
//			.map { article =>
//				// Escape newlines with an improbable string. Otherwise jsoup will replace them with spaces.
//				val escapedNewlines = Jsoup.parse(article.text.replaceAll("\n", "#EscapedNewline")).body.text
//				escapedNewlines.replaceAll("#EscapedNewline", "\n")
//			}.collect
//			.toSet
//		texts shouldEqual originalTexts
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
