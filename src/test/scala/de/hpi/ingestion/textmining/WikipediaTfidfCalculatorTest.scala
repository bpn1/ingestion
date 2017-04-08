package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class WikipediaTfidfCalculatorTest extends FlatSpec with SharedSparkContext with Matchers {
	"Term frequencies" should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val pagesTermFrequencies = articles
			.map(WikipediaTfidfCalculator.countTermFrequencies)
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
			.map(WikipediaTfidfCalculator.countTermFrequencies)
			.collect
			.toSet
		bags shouldEqual termFrequenciesTestSetReference
	}

	"Inverse document frequencies" should "contain as many tokens as document frequencies" in {
		val documentFrequencies = TestData.documentFrequenciesTestSet()
		val documents = 4
		val inverseDocumentFrequencies = documentFrequencies
			.map(df => WikipediaTfidfCalculator.calculateInverseDocumentFrequency(df, documents))
		inverseDocumentFrequencies.size shouldEqual documentFrequencies.size
	}

	they should "be exactly these inverse document frequencies" in {
		val documentFrequencies = TestData.documentFrequenciesTestSet()
		val documents = 4
		val inverseDocumentFrequencies = documentFrequencies
			.map(df => WikipediaTfidfCalculator.calculateInverseDocumentFrequency(df, documents))
		inverseDocumentFrequencies shouldEqual TestData.inverseDocumentFrequenciesTestSet()
	}

	//	"Tf-idf for all parsed Wikipedia articles" should "not be empty" in {
	//		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
	//		val termFrequencies = WikipediaTfidfCalculator
	//			.countTermFrequencies(articles)
	//		val tfidf = WikipediaTfidfCalculator
	//			.calculateTfidf(termFrequencies)
	//		tfidf should not be empty
	//		//		tfidf
	//		//			.map(_.toString)
	//		//			.foreach(println)
	//	}
}
