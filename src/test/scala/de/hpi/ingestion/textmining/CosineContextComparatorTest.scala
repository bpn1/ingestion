package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.{Alias, DFData, FeatureEntry, ParsedWikipediaEntry}
import de.hpi.ingestion.textmining.tokenizer.{CleanCoreNLPTokenizer, IngestionTokenizer}
import org.apache.spark.rdd.RDD

class CosineContextComparatorTest extends FlatSpec with SharedSparkContext with Matchers {
	"Alias probabilities" should "not be empty" in {
		val aliases = sc.parallelize(TestData.finalAliasesSet().toList)
		val pages = CosineContextComparator
			.computeAliasProbabilities(aliases)
			.collect
		pages should not be empty
	}

	they should "be exactly these probabilites" in {
		val aliases = sc.parallelize(TestData.finalAliasesSet().toList)
		val pages = CosineContextComparator
			.computeAliasProbabilities(aliases)
			.collect
			.toMap
		pages shouldEqual TestData.aliasProbabilitiesMap()
	}

	"Cosine similarity between contexts" should "be exactly this value" in {
		val linkContext = TestData.shortLinkContextsTfidfList().head._2
		val pageContext = TestData.articleWordsTfidfMap().head._2
		val cosineSimilarity = CosineContextComparator.calculateCosineSimilarity(linkContext, pageContext)
		cosineSimilarity shouldBe 0.608944778982726
	}

//	"Extracted feature entries for known tfidf values and pages" should "not be empty" in {
//		val contextTfidf = sc.parallelize(TestData.shortLinkContextsTfidfList())
//		val aliasPageProbabilities = TestData.aliasProbabilitiesMap()
//		val articleTfidf = sc.parallelize(TestData.articleWordsTfidfMap().toList)
//		val featureEntries = CosineContextComparator
//			.compareLinksWithArticles(contextTfidf, aliasPageProbabilities, articleTfidf)
//			.collect
//		featureEntries should not be empty
//	}
//
//	they should "be exactly these feature entries" in {
//		val contextTfidf = sc.parallelize(TestData.shortLinkContextsTfidfList())
//		val aliasPageProbabilities = TestData.aliasProbabilitiesMap()
//		val articleTfidf = sc.parallelize(TestData.articleWordsTfidfMap().toList)
//		val featureEntries = CosineContextComparator
//			.compareLinksWithArticles(contextTfidf, aliasPageProbabilities, articleTfidf)
//			.collect
//			.toSet
//		featureEntries shouldEqual TestData.featureEntriesSet()
//	}
//
//	they should "be calculated for trie aliases as well" in {
//		val contextTfidf = sc.parallelize(TestData.shortLinkAndAliasContextsTfidfList())
//		val aliasPageProbabilities = TestData.aliasProbabilitiesMap()
//		val articleTfidf = sc.parallelize(TestData.articleWordsTfidfMap().toList)
//		val featureEntries = CosineContextComparator
//			.compareLinksWithArticles(contextTfidf, aliasPageProbabilities, articleTfidf)
//			.collect
//			.toSet
//		val expected = TestData.featureEntriesWithAliasesSet()
//		featureEntries shouldEqual expected
//	}

//	"Extracted feature entries from links with missing pages" should "be empty" in {
//		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
//		val oldSettings = CosineContextComparator.settings(false)
//		CosineContextComparator.parseConfig()
//
//		val testDocFreqFunction = TestData.docfreqStream("docfreq") _
//		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
//		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
//		val aliases = sc.parallelize(TestData.finalAliasesSet().toList)
//		val input = List(articles).toAnyRDD() ++ List(aliases).toAnyRDD() ++ List(articles).toAnyRDD()
//		val featureEntries = CosineContextComparator
//			.run(input, sc)
//			.fromAnyRDD[FeatureEntry]()
//			.head
//			.collect
//		featureEntries shouldBe empty
//
//		CosineContextComparator.settings = oldSettings
//		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
//	}
//
//	"Extracted feature entries from links with existing pages" should "not be empty" in {
//		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
//		val oldSettings = CosineContextComparator.settings(false)
//		CosineContextComparator.parseConfig()
//
//		val testDocFreqFunction = TestData.docfreqStream("docfreq") _
//		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
//		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
//		val aliases = sc.parallelize(TestData.aliasesWithExistingPagesSet().toList)
//		val input = List(articles).toAnyRDD() ++ List(aliases).toAnyRDD() ++ List(articles).toAnyRDD()
//		val featureEntries = CosineContextComparator
//			.run(input, sc)
//			.fromAnyRDD[FeatureEntry]()
//			.head
//			.collect
//		featureEntries should not be empty
//
//		CosineContextComparator.settings = oldSettings
//		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
//	}
//
//	they should "be the same amount as links" in {
//		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
//		val oldSettings = CosineContextComparator.settings(false)
//		CosineContextComparator.parseConfig()
//
//		val testDocFreqFunction = TestData.docfreqStream("docfreq") _
//		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
//		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
//		val aliases = sc.parallelize(TestData.aliasesWithExistingPagesSet().toList)
//		val input = List(articles).toAnyRDD() ++ List(aliases).toAnyRDD() ++ List(articles).toAnyRDD()
//		val featureEntries = CosineContextComparator
//			.run(input, sc)
//			.fromAnyRDD[FeatureEntry]()
//			.head
//			.collect
//		val linkCount = articles.flatMap(_.allLinks()).count
//		featureEntries.length shouldBe linkCount
//
//		CosineContextComparator.settings = oldSettings
//		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
//	}

//	"Articles" should "be transformed into the correct format" in {
//		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
//		val transformedArticles = CosineContextComparator.transformArticleTfs(articles)
//			.collect
//			.toSet
//		val expectedArticles = TestData.transformedArticles()
//		transformedArticles shouldEqual expectedArticles
//	}
//
//	"Link contexts" should "be transformed into the correct format" in {
//		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
//		val transformedLinks = CosineContextComparator.transformLinkContextTfs(articles)
//			.collect
//			.toSet
//		val expectedLinks = TestData.transformedLinkContexts()
//		transformedLinks shouldEqual expectedLinks
//	}

	"Input split" should "split input into 10 parts" in {
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val aliases = sc.parallelize(TestData.aliasesWithExistingPagesSet().toList)
		val input = List(articles).toAnyRDD() ++ List(aliases).toAnyRDD()
		val splitInput = CosineContextComparator.splitInput(input).toList
		splitInput should have length 10
		splitInput.foreach { case inputPartition =>
			inputPartition should have length 3
		}
	}

	it should "make the Parsed Wikipedia Entries lean" in {
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val aliases = sc.parallelize(TestData.aliasesWithExistingPagesSet().toList)
		val input = List(articles).toAnyRDD() ++ List(aliases).toAnyRDD()
		val splitInput = CosineContextComparator.splitInput(input).toList
		splitInput.foreach { case List(leanArticles, aliases, leanSplit) =>
			leanArticles.asInstanceOf[RDD[ParsedWikipediaEntry]]
				.union(leanSplit.asInstanceOf[RDD[ParsedWikipediaEntry]])
				.collect
				.foreach { entry =>
					entry.templatelinks shouldBe empty
					entry.categorylinks shouldBe empty
					entry.listlinks shouldBe empty
					entry.disambiguationlinks shouldBe empty
					entry.foundaliases shouldBe empty
				}
		}
	}

//	"Term frequencies" should "not be empty" in {
//		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
//		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)
//		val pagesTermFrequencies = articles
//			.map(CosineContextComparator.extractBagOfWords(_, tokenizer))
//		pagesTermFrequencies should not be empty
//		pagesTermFrequencies
//			.map(_.getCounts())
//			.collect
//			.foreach(bag => bag should not be empty)
//	}
//
//	they should "be exactly these term frequencies" in {
//		val expectedTermFrequencies = TestData.termFrequenciesTestSet()
//		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)
//		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
//			.filter(article => expectedTermFrequencies.exists(_._1 == article.title))
//		val tfSet = articles
//			.map(CosineContextComparator.extractBagOfWords(_, tokenizer))
//			.collect
//			.toSet
//		tfSet shouldEqual expectedTermFrequencies.map(_._2)
//	}

	"Wikipedia articles with contexts" should "be exactly these wikipedia articles" in {
		val expectedArticles = TestData.articlesWithContextSet()
		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.filter(article => expectedArticles.exists(_.title == article.title))
			.map(CosineContextComparator.extractArticleContext(_, tokenizer))
			.collect
			.toSet
		articles shouldEqual expectedArticles
	}

	"Link contexts" should "be exactly these bag of words" in {
		val oldSettings = CosineContextComparator.settings
		CosineContextComparator.parseConfig()

		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)
		val linkContexts = TestData.parsedWikipediaTestSet()
			.map(CosineContextComparator.extractLinkContexts(_, tokenizer))
			.flatMap(_.linkswithcontext)
		val expectedContexts = TestData.linksWithContextsTestSet()
		linkContexts shouldEqual expectedContexts

		CosineContextComparator.settings = oldSettings
	}

	they should "be extracted for both text and extendedlinks" in {
		val oldSettings = CosineContextComparator.settings
		CosineContextComparator.parseConfig()

		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)
		val linkContexts = TestData.parsedWikipediaExtendedLinksTestSet()
			.map(CosineContextComparator.extractLinkContexts(_, tokenizer))
			.flatMap(_.linkswithcontext)
		val expectedContexts = TestData.linksWithContextsTestSet()
		linkContexts shouldEqual expectedContexts

		CosineContextComparator.settings = oldSettings
	}

	"Articles with link context" should "contain exactly these link contexts" in {
		val oldSettings = CosineContextComparator.settings
		CosineContextComparator.parseConfig()

		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)
		val expectedLinks = TestData.linksWithContextsTestSet()
		val enrichedLinks = TestData.parsedWikipediaTestSet()
			.map(CosineContextComparator.extractLinkContexts(_, tokenizer))
			.flatMap(_.linkswithcontext)
		enrichedLinks shouldEqual expectedLinks

		CosineContextComparator.settings = oldSettings
	}

	they should "be exactly these articles" in {
		val oldSettings = CosineContextComparator.settings
		CosineContextComparator.parseConfig()

		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)
		val enrichedLinkArticles = TestData.parsedWikipediaTestSet()
			.map(CosineContextComparator.extractLinkContexts(_, tokenizer))
		val expectedArticles = TestData.articlesWithLinkContextsSet()
		enrichedLinkArticles shouldEqual expectedArticles

		CosineContextComparator.settings = oldSettings
	}
}
