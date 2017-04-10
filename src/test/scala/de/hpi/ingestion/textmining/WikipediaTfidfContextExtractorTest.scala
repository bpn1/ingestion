package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class WikipediaTfidfContextExtractorTest extends FlatSpec with SharedSparkContext with Matchers {
	"Term frequencies" should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val pagesTermFrequencies = articles
			.map(WikipediaTfidfContextExtractor
				.extractBagOfWords(_, new CleanCoreNLPTokenizer, new AccessibleGermanStemmer))
		pagesTermFrequencies should not be empty
		pagesTermFrequencies
			.map(_._2.getCounts())
			.collect
			.foreach(bag => bag should not be empty)
	}

	they should "be exactly these term frequencies" in {
		val expectedTermFrequencies = TestData.normalizedTermFrequenciesTestSet()
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.filter(article => expectedTermFrequencies.exists(_._1 == article.title))
		val tfSet = articles
			.map(WikipediaTfidfContextExtractor
				.extractBagOfWords(_, new CleanCoreNLPTokenizer, new AccessibleGermanStemmer))
			.collect
			.toSet
		tfSet shouldEqual expectedTermFrequencies
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
		val documentFrequencies = sc.parallelize(TestData.documentFrequenciesTestSet().toList)
		val contexts = WikipediaTfidfContextExtractor
			.calculateArticleTfidf(articles, documentFrequencies, documentFrequencyThreshold)
			.collect
		contexts should not be empty
		contexts.foreach(context => context.tokens should not be empty)
	}

	they should "be exactly these contexts" in {
		val documentFrequencyThreshold = 1
		val articles = sc.parallelize(
			TestData.parsedWikipediaTestSet()
				.toList
				.filter(_.title != "Streitberg (Brachttal)"))
		val documentFrequencies = sc.parallelize(TestData.stemmedDocumentFrequenciesTestSet().toList)
		val contexts = WikipediaTfidfContextExtractor
			.calculateArticleTfidf(articles, documentFrequencies, documentFrequencyThreshold)
			.collect
			.toSet
		contexts shouldEqual TestData.tfidfContextsTestSet()
	}

	"Tf-idf contexts for parsed Wikipedia articles with missing document frequencies" should
		"be exactly these contexts" in {
		val documentFrequencyThreshold = 2
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.filter(_.title != "Streitberg (Brachttal)")
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesTestSet().toList)
		val contexts = WikipediaTfidfContextExtractor
			.calculateArticleTfidf(articles, documentFrequencies, documentFrequencyThreshold)
			.collect
			.toSet
		contexts shouldEqual TestData.tfidfContextsTestSet()
	}

}
