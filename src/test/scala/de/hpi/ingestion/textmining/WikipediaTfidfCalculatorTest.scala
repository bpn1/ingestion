package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class WikipediaTfidfCalculatorTest extends FlatSpec with SharedSparkContext with Matchers {
	"Term frequencies" should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val pagesTermFrequencies = WikipediaTfidfCalculator.countTermFrequencies(articles)
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
		val bags = WikipediaTfidfCalculator.countTermFrequencies(articles)
			.collect
			.toSet
		bags shouldEqual termFrequenciesTestSetReference
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
