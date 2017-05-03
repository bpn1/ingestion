package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.FeatureEntry

class CosineContextComparatorTest extends FlatSpec with SharedSparkContext with Matchers {
	"Inverse document frequencies" should "contain as many tokens as document frequencies" in {
		val documentFrequencies = TestData.documentFrequenciesTestSet()
		val numDocuments = 4
		val inverseDocumentFrequencies = documentFrequencies
			.map(CosineContextComparator.calculateIdf(_, numDocuments))
		inverseDocumentFrequencies.size shouldEqual documentFrequencies.size
	}

	they should "be exactly these inverse document frequencies" in {
		val documentFrequencies = TestData.documentFrequenciesTestSet()
		val numDocuments = 4
		val inverseDocumentFrequencies = documentFrequencies
			.map(CosineContextComparator.calculateIdf(_, numDocuments))
		inverseDocumentFrequencies shouldEqual TestData.inverseDocumentFrequenciesSet()
	}

	"Tf-idf contexts for parsed Wikipedia articles with complete document frequencies" should "not be empty" in {
		val documentFrequencyThreshold = 2
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val numDocuments = articles.count
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesSet().toList)
		val contexts = CosineContextComparator
			.calculateArticleTfidf(articles, documentFrequencies, numDocuments, documentFrequencyThreshold)
			.collect
		contexts should not be empty
		contexts.foreach(context => context._2 should not be empty)
	}

	they should "be exactly these contexts" in {
		val documentFrequencyThreshold = 2
		val articles = sc.parallelize(
			TestData.articlesWithCompleteContexts()
				.toList
				.filter(_.title != "Streitberg (Brachttal)"))
		val numDocuments = articles.count
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesSmallSet().toList)
		val contexts = CosineContextComparator
			.calculateArticleTfidf(articles, documentFrequencies, numDocuments, documentFrequencyThreshold)
			.collect
			.toSet
		contexts shouldEqual TestData.tfidfContextsSet()
	}

	"Tf-idf contexts for parsed Wikipedia articles with missing document frequencies" should
		"be exactly these contexts" in {
		val documentFrequencyThreshold = 2
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
			.filter(_.title != "Streitberg (Brachttal)")
		val numDocuments = articles.count
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesSmallSet().toList)
		val contexts = CosineContextComparator
			.calculateArticleTfidf(articles, documentFrequencies, numDocuments, documentFrequencyThreshold)
			.collect
			.toSet
		contexts shouldEqual TestData.tfidfContextsSet()
	}

	"Tf-Idf of link contexts" should "exist" in {
		val documentFrequencyThreshold = 2
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val numDocuments = articles.count
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesSet().toList)
		val linkContextValues = CosineContextComparator
			.calculateLinkContextsTfidf(articles, documentFrequencies, numDocuments, documentFrequencyThreshold)
			.collect
		linkContextValues should not be empty
	}

	they should "be exactly these tf-Idf values (disregarding the contexts)" in {
		val documentFrequencyThreshold = 2
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val numDocuments = articles.count
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesSet().toList)
		val linkContextValues = CosineContextComparator
			.calculateLinkContextsTfidf(articles, documentFrequencies, numDocuments, documentFrequencyThreshold)
			.map { case (link, tfidfContext) =>
				val smallLink = link
				smallLink.context = Map() // allows smaller test data
				(smallLink, tfidfContext)
			}
			.collect
			.toSet
		val expectedTfidf = TestData.linkContextsTfidf()
		linkContextValues shouldBe expectedTfidf
	}

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
			.toSet
		pages shouldEqual TestData.aliasProbabilitiesSet()
	}

	"Cosine similarity between contexts" should "be exactly this value" in {
		val linkContext = TestData.shortLinkContextsTfidfList().head._2
		val pageContext = TestData.articleWordsTfidfMap().head._2
		val cosineSimilarity = CosineContextComparator.calculateCosineSimilarity(linkContext, pageContext)
		cosineSimilarity shouldBe 0.608944778982726
	}

	"Extracted feature entries for known tfidf values and pages" should "not be empty" in {
		val contextTfidf = sc.parallelize(TestData.shortLinkContextsTfidfList())
		val aliasPageProbabilities = sc.parallelize(TestData.aliasProbabilitiesSet().toList)
		val articleTfidf = sc.parallelize(TestData.articleWordsTfidfMap().toList)
		val featureEntries = CosineContextComparator
			.compareLinksWithArticles(contextTfidf, aliasPageProbabilities, articleTfidf)
			.collect
		featureEntries should not be empty
	}

	they should "be exactly these feature entries" in {
		val contextTfidf = sc.parallelize(TestData.shortLinkContextsTfidfList())
		val aliasPageProbabilities = sc.parallelize(TestData.aliasProbabilitiesSet().toList)
		val articleTfidf = sc.parallelize(TestData.articleWordsTfidfMap().toList)
		val featureEntries = CosineContextComparator
			.compareLinksWithArticles(contextTfidf, aliasPageProbabilities, articleTfidf)
			.collect
			.map(_.copy(id = null))
			.toSet
		featureEntries shouldEqual TestData.featureEntriesSet()
	}

	"Extracted feature entries from links with missing pages" should "be empty" in {
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesSet().toList)
		val aliases = sc.parallelize(TestData.finalAliasesSet().toList)
		val input = List(articles).toAnyRDD() ++ List(documentFrequencies).toAnyRDD() ++ List(aliases).toAnyRDD()
		val featureEntries = CosineContextComparator
			.run(input, sc)
			.fromAnyRDD[FeatureEntry]()
			.head
			.collect
		featureEntries shouldBe empty
	}

	"Extracted feature entries from links with existing pages" should "not be empty" in {
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesSet().toList)
		val aliases = sc.parallelize(TestData.aliasesWithExistingPagesSet().toList)
		val input = List(articles).toAnyRDD() ++ List(documentFrequencies).toAnyRDD() ++ List(aliases).toAnyRDD()
		val featureEntries = CosineContextComparator
			.run(input, sc)
			.fromAnyRDD[FeatureEntry]()
			.head
			.collect
		featureEntries should not be empty
	}

	they should "be the same amount as links" in {
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesSet().toList)
		val aliases = sc.parallelize(TestData.aliasesWithExistingPagesSet().toList)
		val input = List(articles).toAnyRDD() ++ List(documentFrequencies).toAnyRDD() ++ List(aliases).toAnyRDD()
		val featureEntries = CosineContextComparator
			.run(input, sc)
			.fromAnyRDD[FeatureEntry]()
			.head
			.collect
		val linkCount = articles.flatMap(_.allLinks()).count
		featureEntries.length shouldBe linkCount
	}
}
