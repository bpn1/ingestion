package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class WikipediaTfidfContextExtractorTest extends FlatSpec with SharedSparkContext with Matchers {
	"Term frequencies" should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val pagesTermFrequencies = articles
			.map(WikipediaTfidfContextExtractor.countTermFrequencies)
		pagesTermFrequencies should not be empty
		pagesTermFrequencies
			.map(_.tokens.getCounts())
			.collect
			.foreach(bag => bag should not be empty)
	}

	they should "be exactly these term frequencies" in {
		val termFrequenciesTestSetReference = TestData.termFrequenciesTestSet()
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.filter { article =>
				termFrequenciesTestSetReference.exists(termFrequencies => termFrequencies.page == article.title)
			}
		val bags = articles
			.map(WikipediaTfidfContextExtractor.countTermFrequencies)
			.collect
			.toSet
		bags shouldEqual termFrequenciesTestSetReference
	}

	"Inverse document frequencies" should "contain as many tokens as document frequencies" in {
		val documentFrequencies = TestData.documentFrequenciesTestSet()
		val documents = 4
		val inverseDocumentFrequencies = documentFrequencies
			.map(df => WikipediaTfidfContextExtractor.calculateInverseDocumentFrequency(df, documents))
		inverseDocumentFrequencies.size shouldEqual documentFrequencies.size
	}

	they should "be exactly these inverse document frequencies" in {
		val documentFrequencies = TestData.documentFrequenciesTestSet()
		val documents = 4
		val inverseDocumentFrequencies = documentFrequencies
			.map(df => WikipediaTfidfContextExtractor.calculateInverseDocumentFrequency(df, documents))
		inverseDocumentFrequencies shouldEqual TestData.inverseDocumentFrequenciesTestSet()
	}

	"Tf-idf contexts for parsed Wikipedia articles with complete document frequencies" should "not be empty" in {
		val documentFrequencyThreshold = 1
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val documentFrequencies = sc.parallelize(TestData.documentFrequenciesTestSet().toList)
		val contexts = WikipediaTfidfContextExtractor
			.calculateTfidfContexts(articles, documentFrequencies, documentFrequencyThreshold)
			.collect
		contexts should not be empty
		contexts.foreach(context => context.tokens should not be empty)
	}

	they should "be exactly these contexts" in {
		val documentFrequencyThreshold = 1
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.filter { article => article.title != "Streitberg (Brachttal)" }
		val documentFrequencies = sc.parallelize(TestData.stemmedDocumentFrequenciesTestSet().toList)
		val contexts = WikipediaTfidfContextExtractor
			.calculateTfidfContexts(articles, documentFrequencies, documentFrequencyThreshold)
			.collect
			.toSet
		contexts shouldEqual TestData.tfidfContextsTestSet()
	}

	"Tf-idf contexts for parsed Wikipedia articles with missing document frequencies" should
		"be exactly these contexts" in {
		val documentFrequencyThreshold = 2
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.filter { article => article.title != "Streitberg (Brachttal)" }
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesTestSet().toList)
		val contexts = WikipediaTfidfContextExtractor
			.calculateTfidfContexts(articles, documentFrequencies, documentFrequencyThreshold)
			.collect
			.toSet
		contexts shouldEqual TestData.tfidfContextsTestSet()
	}
}
