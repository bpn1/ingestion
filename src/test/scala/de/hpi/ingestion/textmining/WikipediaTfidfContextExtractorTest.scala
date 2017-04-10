package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class WikipediaTfidfContextExtractorTest extends FlatSpec with SharedSparkContext with Matchers {
	"Term frequencies" should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val pagesTermFrequencies = articles
			.map(WikipediaTfidfContextExtractor.extractBagOfWords)
		pagesTermFrequencies should not be empty
		pagesTermFrequencies
			.map(_.getCounts())
			.collect
			.foreach(bag => bag should not be empty)
	}

	they should "be exactly these term frequencies" in {
		val expectedTermFrequencies = TestData.normalizedTermFrequenciesTestSet()
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.filter(article => expectedTermFrequencies.exists(_._1 == article.title))
		val tfSet = articles
			.map(WikipediaTfidfContextExtractor.extractBagOfWords)
			.collect
			.toSet
		tfSet shouldEqual expectedTermFrequencies.map(_._2)
	}

	"Inverse document frequencies" should "contain as many tokens as document frequencies" in {
		val documentFrequencies = TestData.documentFrequenciesTestSet()
		val numDocuments = 4
		val inverseDocumentFrequencies = documentFrequencies
			.map(WikipediaTfidfContextExtractor.calculateIdf(_, numDocuments))
		inverseDocumentFrequencies.size shouldEqual documentFrequencies.size
	}

	they should "be exactly these inverse document frequencies" in {
		val documentFrequencies = TestData.documentFrequenciesTestSet()
		val numDocuments = 4
		val inverseDocumentFrequencies = documentFrequencies
			.map(WikipediaTfidfContextExtractor.calculateIdf(_, numDocuments))
		inverseDocumentFrequencies shouldEqual TestData.inverseDocumentFrequenciesTestSet()
	}

	"Tf-idf contexts for parsed Wikipedia articles with complete document frequencies" should "not be empty" in {
		val documentFrequencyThreshold = 1
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val numDocuments = articles.count
		val documentFrequencies = sc.parallelize(TestData.documentFrequenciesTestSet().toList)
		val contexts = WikipediaTfidfContextExtractor
			.calculateArticleTfidf(articles, documentFrequencies, numDocuments, documentFrequencyThreshold)
			.collect
		contexts should not be empty
		contexts.foreach(context => context._2 should not be empty)
	}

	they should "be exactly these contexts" in {
		val documentFrequencyThreshold = 1
		val articles = sc.parallelize(
			TestData.parsedWikipediaTestSet()
				.toList
				.filter(_.title != "Streitberg (Brachttal)"))
		val numDocuments = articles.count
		val documentFrequencies = sc.parallelize(TestData.stemmedDocumentFrequenciesTestSet().toList)
		val contexts = WikipediaTfidfContextExtractor
			.calculateArticleTfidf(articles, documentFrequencies, numDocuments, documentFrequencyThreshold)
			.collect
			.toSet
		contexts shouldEqual TestData.tfidfContextsTestSet()
	}

	"Tf-idf contexts for parsed Wikipedia articles with missing document frequencies" should
		"be exactly these contexts" in {
		val documentFrequencyThreshold = 2
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.filter(_.title != "Streitberg (Brachttal)")
		val numDocuments = articles.count
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesTestSet().toList)
		val contexts = WikipediaTfidfContextExtractor
			.calculateArticleTfidf(articles, documentFrequencies, numDocuments, documentFrequencyThreshold)
			.collect
			.toSet
		contexts shouldEqual TestData.tfidfContextsTestSet()
	}

	"Link contexts" should "be extracted" in {
		val linkContexts = TestData.parsedWikipediaTestSet()
			.flatMap(WikipediaTfidfContextExtractor.extractLinkContexts)
		val expectedContexts = TestData.linkContexts()
		linkContexts shouldBe expectedContexts
	}

	"Tf-Idf of link contexts" should "be extactly these tf-Idf values" in {
		val documentFrequencyThreshold = 1
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val numDocuments = articles.count
		val documentFrequencies = sc.parallelize(TestData.stemmedDocumentFrequenciesTestSet().toList)
		val linkContextValues = WikipediaTfidfContextExtractor
			.calculateContextTfidf(articles, documentFrequencies, numDocuments, documentFrequencyThreshold)
			.collect
			.toSet
		val expectedTfidf = TestData.linkContextsTfidf()
		linkContextValues shouldBe expectedTfidf
	}

}
