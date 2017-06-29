package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.{FeatureEntry, ParsedWikipediaEntry}
import org.apache.spark.rdd.RDD

class CosineContextComparatorTest extends FlatSpec with SharedSparkContext with Matchers {
	"Inverse document frequencies" should "contain as many tokens as document frequencies" in {
		val documentFrequencies = TestData.documentFrequenciesSet()
		val numDocuments = 4
		val inverseDocumentFrequencies = documentFrequencies
			.map(CosineContextComparator.calculateIdf(_, numDocuments))
		inverseDocumentFrequencies.size shouldEqual documentFrequencies.size
	}

	they should "be exactly these inverse document frequencies" in {
		val documentFrequencies = TestData.documentFrequenciesSet()
		val numDocuments = 4
		val inverseDocumentFrequencies = documentFrequencies
			.map(CosineContextComparator.calculateIdf(_, numDocuments))
		inverseDocumentFrequencies shouldEqual TestData.inverseDocumentFrequenciesSet()
	}

	they should "be read and calculated from a file" in {
		val oldSettings = CosineContextComparator.settings(false)
		CosineContextComparator.parseConfig()
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val testDocFreqFunction = TestData.docfreqStream("smalldocfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction

		val numDocuments = 4
		val idfMap = CosineContextComparator.inverseDocumentFrequencies(numDocuments.toLong)
		val expectedIdfs = TestData.inverseDocumentFrequenciesSet().toMap
		idfMap shouldEqual expectedIdfs

		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
		CosineContextComparator.settings = oldSettings
	}

	"Tf-idf contexts for parsed Wikipedia articles with complete document frequencies" should "not be empty" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldSettings = CosineContextComparator.settings(false)
		CosineContextComparator.parseConfig()

		val testDocFreqFunction = TestData.docfreqStream("docfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val numDocuments = articles.count
		val contexts = CosineContextComparator
			.calculateTfidf(
				CosineContextComparator.transformArticleTfs(articles),
				numDocuments,
				CosineContextComparator.defaultIdf(numDocuments, 2))
			.collect
		contexts should not be empty
		contexts.foreach(context => context._2 should not be empty)

		CosineContextComparator.settings = oldSettings
		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	they should "be exactly these contexts" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldSettings = CosineContextComparator.settings(false)
		CosineContextComparator.parseConfig()

		val testDocFreqFunction = TestData.docfreqStream("docfreq2") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val articles = sc.parallelize(
			TestData.articlesWithCompleteContexts()
				.toList
				.filter(_.title != "Streitberg (Brachttal)"))
		val numDocuments = articles.count
		val contexts = CosineContextComparator
			.calculateTfidf(
				CosineContextComparator.transformArticleTfs(articles),
				numDocuments,
				CosineContextComparator.defaultIdf(numDocuments, 2))
			.collect
			.toSet
		contexts shouldEqual TestData.tfidfContextsSet()

		CosineContextComparator.settings = oldSettings
		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	"Tf-idf contexts for parsed Wikipedia articles with missing document frequencies" should
		"be exactly these contexts" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldSettings = CosineContextComparator.settings(false)
		CosineContextComparator.parseConfig()

		val testDocFreqFunction = TestData.docfreqStream("docfreq2") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
			.filter(_.title != "Streitberg (Brachttal)")
		val numDocuments = articles.count
		val contexts = CosineContextComparator
			.calculateTfidf(
				CosineContextComparator.transformArticleTfs(articles),
				numDocuments,
				CosineContextComparator.defaultIdf(numDocuments, 2))
			.collect
			.toSet
		contexts shouldEqual TestData.tfidfContextsSet()

		CosineContextComparator.settings = oldSettings
		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	"Tf-Idf of link contexts" should "exist" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldSettings = CosineContextComparator.settings(false)
		CosineContextComparator.parseConfig()

		val testDocFreqFunction = TestData.docfreqStream("docfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val numDocuments = articles.count
		val linkContextValues = CosineContextComparator
			.calculateTfidf(
				CosineContextComparator.transformLinkContextTfs(articles),
				numDocuments,
				CosineContextComparator.defaultIdf(numDocuments, 2))
			.collect
		linkContextValues should not be empty

		CosineContextComparator.settings = oldSettings
		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	they should "be exactly these tf-Idf values (disregarding the contexts)" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldSettings = CosineContextComparator.settings(false)
		CosineContextComparator.parseConfig()

		val testDocFreqFunction = TestData.docfreqStream("docfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val numDocuments = articles.count
		val linkContextValues = CosineContextComparator
			.calculateTfidf(
				CosineContextComparator.transformLinkContextTfs(articles),
				numDocuments,
				CosineContextComparator.defaultIdf(numDocuments, 2))
			.map { case (link, tfidfContext) => (link.copy(context = Map()), tfidfContext) }
			.collect
			.toList
			.sortBy(_._1.alias)
		val expectedTfidf = TestData.linkContextsTfidfList()
		linkContextValues shouldBe expectedTfidf

		CosineContextComparator.settings = oldSettings
		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	they should "compute the tf-Idf values for the linkswithcontext and trialiases contexts" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldSettings = CosineContextComparator.settings
		CosineContextComparator.parseConfig()

		val testDocFreqFunction = TestData.docfreqStream("docfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val articles = sc.parallelize(TestData.articlesWithLinkAndAliasContexts().toList)
		val numDocuments = articles.count
		val linkContextValues = CosineContextComparator
			.calculateTfidf(
				CosineContextComparator.transformLinkContextTfs(articles),
				numDocuments,
				CosineContextComparator.defaultIdf(numDocuments, 2))
			.map { case (link, tfidfContext) => (link.copy(context = Map()), tfidfContext) }
			.collect
			.toSet
		val expectedTfidf = TestData.linkContextsTfidfWithTrieAliases()
		linkContextValues shouldBe expectedTfidf

		CosineContextComparator.settings = oldSettings
		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	"Link and page scores" should "not be empty" in {
		val aliases = sc.parallelize(TestData.finalAliasesSet().toList)
		val pages = CosineContextComparator
			.computeAliasPageScores(aliases)
			.collect
		pages should not be empty
	}

	they should "be exactly these scores" in {
		val aliases1 = sc.parallelize(TestData.finalAliasesSet().toList)
		val aliases2 = sc.parallelize(List(TestData.aliasWithManyPages()))

		val pages1 = CosineContextComparator
			.computeAliasPageScores(aliases1)
			.collect
			.toMap
		val pages2 = CosineContextComparator
			.computeAliasPageScores(aliases2)
			.collect
			.toMap
			.map(alias => (alias._1, alias._2.toSet))

		pages1 shouldEqual TestData.aliasPagesScoresMap()
		val expectedPages = TestData.singleAliasManyPagesScoresMap()
			.map(alias => (alias._1, alias._2.toSet))
		pages2 shouldEqual expectedPages
	}

	"Cosine similarity between contexts" should "be exactly this value" in {
		val linkContext = TestData.shortLinkContextsTfidfList().head._2
		val pageContext = TestData.articleWordsTfidfMap().head._2
		val cosineSimilarity = CosineContextComparator.calculateCosineSimilarity(linkContext, pageContext)
		cosineSimilarity shouldBe 0.608944778982726
	}

	it should "be 0 if one vector is empty" in {
		val linkContext = TestData.shortLinkContextsTfidfList().head._2
		val pageContext = Map[String, Double]()
		val cosineSimilarity = CosineContextComparator.calculateCosineSimilarity(linkContext, pageContext)
		cosineSimilarity shouldBe 0.0

	}

	"Extracted feature entries for known tfidf values and pages" should "not be empty" in {
		val contextTfidf = sc.parallelize(TestData.shortLinkContextsTfidfList())
		val aliasPageScores = sc.broadcast(TestData.aliasPagesScoresMap())
		val articleTfidf = sc.parallelize(TestData.articleWordsTfidfMap().toList)
		val featureEntries = CosineContextComparator
			.compareLinksWithArticles(contextTfidf, aliasPageScores, articleTfidf)
			.collect
		featureEntries should not be empty
	}

	they should "be exactly these feature entries" in {
		val contextTfidf = sc.parallelize(
			TestData.shortLinkContextsTfidfList() ++ TestData.deadAliasContextsTfidfList())
		val aliasPageScores = sc.broadcast(TestData.aliasPagesScoresMap())
		val articleTfidf = sc.parallelize(TestData.articleWordsTfidfMap().toList)
		val featureEntries = CosineContextComparator
			.compareLinksWithArticles(contextTfidf, aliasPageScores, articleTfidf)
			.collect
			.toList
			.sortBy(featureEntry => (featureEntry.article, featureEntry.offset, featureEntry.entity))
		featureEntries shouldEqual TestData.featureEntriesList()
	}

	they should "be calculated for trie aliases as well" in {
		val contextTfidf = sc.parallelize(
			TestData.shortLinkAndAliasContextsTfidfList() ++ TestData.deadAliasContextsTfidfList())
		val aliasPageScores = sc.broadcast(TestData.aliasPagesScoresMap())
		val articleTfidf = sc.parallelize(TestData.articleWordsTfidfMap().toList)
		val featureEntries = CosineContextComparator
			.compareLinksWithArticles(contextTfidf, aliasPageScores, articleTfidf)
			.collect
			.toSet
		val expected = TestData.featureEntriesWithAliasesSet()
		featureEntries shouldEqual expected
	}

	"Feature entries" should "be calculated correctly" in {
		val contextTfidf = sc.parallelize(TestData.singleAliasLinkList())
		val aliasPageScores = sc.broadcast(TestData.singleAliasPageScoresMap())
		val articleTfidf = sc.parallelize(TestData.emptyArticlesTfidfMap().toList)
		val featureEntries = CosineContextComparator
			.compareLinksWithArticles(contextTfidf, aliasPageScores, articleTfidf)
			.collect
			.toList
			.sortBy(featureEntry => (featureEntry.article, featureEntry.offset, featureEntry.entity))
		val expected = TestData.featureEntriesForSingleAliasList()
		featureEntries shouldEqual expected
	}

	they should "be calculated correctly for many possible entities" in {
		val contextTfidf = sc.parallelize(List(TestData.singleAliasLinkList().head))
		val aliasPageScores = sc.broadcast(TestData.singleAliasManyPagesScoresMap())
		val articleTfidf = sc.parallelize(TestData.emptyArticlesTfidfMap().toList)
		val featureEntries = CosineContextComparator
			.compareLinksWithArticles(contextTfidf, aliasPageScores, articleTfidf)
			.collect
			.toList
			.sortBy(featureEntry => (featureEntry.entity_score.rank, featureEntry.entity))
		val expected = TestData.featureEntriesForManyPossibleEntitiesList()
		featureEntries shouldEqual expected
	}

	they should "be calculated correctly when using the run method" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldThresh = DocumentFrequencyCounter.leastSignificantDocumentFrequency
		DocumentFrequencyCounter.leastSignificantDocumentFrequency = 2
		val oldSettings = CosineContextComparator.settings(false)
		CosineContextComparator.parseConfig()

		val testDocFreqFunction = TestData.docfreqStream("docfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val articles = sc.parallelize(TestData.articlesForSingleAlias().toList)
		val aliases = sc.parallelize(List(TestData.aliasWithManyPages()))
		val input = List(articles).toAnyRDD() ++ List(aliases).toAnyRDD() ++ List(articles).toAnyRDD()
		val featureEntries = CosineContextComparator
			.run(input, sc)
			.fromAnyRDD[FeatureEntry]()
			.head
			.collect
			.toList
			.sortBy(featureEntry => (featureEntry.entity_score.rank, featureEntry.entity))
		featureEntries shouldEqual TestData.featureEntriesForManyPossibleEntitiesList()

		CosineContextComparator.settings = oldSettings
		DocumentFrequencyCounter.leastSignificantDocumentFrequency = oldThresh
		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	"Extracted feature entries from links with missing pages" should "be empty" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldSettings = CosineContextComparator.settings(false)
		CosineContextComparator.parseConfig()

		val testDocFreqFunction = TestData.docfreqStream("docfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val aliases = sc.parallelize(TestData.finalAliasesSet().toList)
		val input = List(articles).toAnyRDD() ++ List(aliases).toAnyRDD() ++ List(articles).toAnyRDD()
		val featureEntries = CosineContextComparator
			.run(input, sc)
			.fromAnyRDD[FeatureEntry]()
			.head
			.collect
		featureEntries shouldBe empty

		CosineContextComparator.settings = oldSettings
		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	"Extracted feature entries from links with existing pages" should "not be empty" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldThresh = DocumentFrequencyCounter.leastSignificantDocumentFrequency
		DocumentFrequencyCounter.leastSignificantDocumentFrequency = 2
		val oldSettings = CosineContextComparator.settings(false)
		CosineContextComparator.parseConfig()

		val testDocFreqFunction = TestData.docfreqStream("docfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val aliases = sc.parallelize(TestData.aliasesWithExistingPagesSet().toList)
		val input = List(articles).toAnyRDD() ++ List(aliases).toAnyRDD() ++ List(articles).toAnyRDD()
		val featureEntries = CosineContextComparator
			.run(input, sc)
			.fromAnyRDD[FeatureEntry]()
			.head
			.collect
		featureEntries should not be empty

		CosineContextComparator.settings = oldSettings
		DocumentFrequencyCounter.leastSignificantDocumentFrequency = oldThresh
		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	they should "be the same amount as links" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldThresh = DocumentFrequencyCounter.leastSignificantDocumentFrequency
		DocumentFrequencyCounter.leastSignificantDocumentFrequency = 2
		val oldSettings = CosineContextComparator.settings(false)
		CosineContextComparator.parseConfig()

		val testDocFreqFunction = TestData.docfreqStream("docfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val aliases = sc.parallelize(TestData.aliasesWithExistingPagesSet().toList)
		val input = List(articles).toAnyRDD() ++ List(aliases).toAnyRDD() ++ List(articles).toAnyRDD()
		val featureEntries = CosineContextComparator
			.run(input, sc)
			.fromAnyRDD[FeatureEntry]()
			.head
			.collect
		val linkCount = articles.flatMap(_.allLinks()).count
		featureEntries.length shouldBe linkCount

		CosineContextComparator.settings = oldSettings
		DocumentFrequencyCounter.leastSignificantDocumentFrequency = oldThresh
		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	"Articles" should "be transformed into the correct format" in {
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val transformedArticles = CosineContextComparator.transformArticleTfs(articles)
			.collect
			.toSet
		val expectedArticles = TestData.transformedArticles()
		transformedArticles shouldEqual expectedArticles
	}

	"Link contexts" should "be transformed into the correct format" in {
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val transformedLinks = CosineContextComparator.transformLinkContextTfs(articles)
			.collect
			.toSet
		val expectedLinks = TestData.transformedLinkContexts()
		transformedLinks shouldEqual expectedLinks
	}

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
}
