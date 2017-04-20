package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

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
		inverseDocumentFrequencies shouldEqual TestData.inverseDocumentFrequenciesTestSet()
	}

	"Tf-idf contexts for parsed Wikipedia articles with complete document frequencies" should "not be empty" in {
		val documentFrequencyThreshold = 2
		val articles = sc.parallelize(TestData.articlesWithContextTestSet().toList)
		val numDocuments = articles.count
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesTestSet().toList)
		val contexts = CosineContextComparator
			.calculateArticleTfidf(articles, documentFrequencies, numDocuments, documentFrequencyThreshold)
			.collect
		contexts should not be empty
		contexts.foreach(context => context._2 should not be empty)
	}

	they should "be exactly these contexts" in {
		val documentFrequencyThreshold = 2
		val articles = sc.parallelize(
			TestData.articlesWithContextTestSet()
				.toList
				.filter(_.title != "Streitberg (Brachttal)"))
		val numDocuments = articles.count
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesSmallTestSet().toList)
		val contexts = CosineContextComparator
			.calculateArticleTfidf(articles, documentFrequencies, numDocuments, documentFrequencyThreshold)
			.collect
			.toSet
		contexts shouldEqual TestData.tfidfContextsTestSet()
	}

	"Tf-idf contexts for parsed Wikipedia articles with missing document frequencies" should
		"be exactly these contexts" in {
		val documentFrequencyThreshold = 2
		val articles = sc.parallelize(TestData.articlesWithContextTestSet().toList)
			.filter(_.title != "Streitberg (Brachttal)")
		val numDocuments = articles.count
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesSmallTestSet().toList)
		val contexts = CosineContextComparator
			.calculateArticleTfidf(articles, documentFrequencies, numDocuments, documentFrequencyThreshold)
			.collect
			.toSet
		contexts shouldEqual TestData.tfidfContextsTestSet()
	}

	"Tf-Idf of link contexts" should "exist" in {
		val documentFrequencyThreshold = 2
		val articles = sc.parallelize(TestData.articlesWithLinkContextsTestSet().toList)
		val numDocuments = articles.count
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesTestSet().toList)
		val linkContextValues = CosineContextComparator
			.calculateLinkContextsTfidf(articles, documentFrequencies, numDocuments, documentFrequencyThreshold)
			.collect
		linkContextValues should not be empty
	}

	they should "be exactly these tf-Idf values (disregarding the contexts)" in {
		val documentFrequencyThreshold = 2
		val articles = sc.parallelize(TestData.articlesWithLinkContextsTestSet().toList)
		val numDocuments = articles.count
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesTestSet().toList)
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
			.joinablePages(aliases)
			.collect
		pages should not be empty
	}

	they should "be exactly these probabilites" in {
		val aliases = sc.parallelize(TestData.finalAliasesSet().toList)
		val pages = CosineContextComparator
			.joinablePages(aliases)
			.collect
			.toSet
		pages shouldEqual TestData.aliasProbabilitiesSet()
	}

	"Extracted feature entries for known tfidf values and pages" should "not be empty" in {
		val articleTfidf = sc.parallelize(TestData.tfidfContextsTestSet().toList)
		val pages = sc.parallelize(TestData.aliasProbabilitiesSet().toList)
		val contextTfidf = sc.parallelize(TestData.linkContextsTfidf().toList)
		val featureEntries = CosineContextComparator
			.compareLinksWithArticles(contextTfidf, pages, articleTfidf)
			.collect
		featureEntries should not be empty
	}

	"Extracted feature entries" should "not be empty" in {
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesTestSet().toList)
		val aliases = sc.parallelize(TestData.finalAliasesSet().toList)
		val featureEntries = CosineContextComparator
			.run(articles, documentFrequencies, aliases)
			.collect
		featureEntries should not be empty
	}
}
