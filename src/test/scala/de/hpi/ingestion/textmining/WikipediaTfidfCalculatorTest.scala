package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class WikipediaTfidfCalculatorTest extends FlatSpec with SharedSparkContext with Matchers {
	"Bags of words for articles" should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		WikipediaTfidfCalculator.articlesToBagsOfWords(articles)
			.map(_.tokens.getCounts())
			.collect
			.foreach(bag => bag should not be empty)
	}

	they should "be exactly these bags of words" in {
		val termFrequenciesTestSetReference = TestData.termFrequenciesTestSet()
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.filter { article =>
				termFrequenciesTestSetReference.exists(termFrequencies => termFrequencies.page == article.title)
			}
		val bags = WikipediaTfidfCalculator.articlesToBagsOfWords(articles)
			.collect
			.toSet
		bags shouldEqual termFrequenciesTestSetReference
	}

	//	"Term frequencies" should "not be empty" in {
	//		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
	//		val termFrequencies = WikipediaTfidfCalculator
	//			.countTermFrequencies(articles)
	//		termFrequencies should not be empty
	//		//		termFrequencies
	//		//			.map(_.toArray.toList.filter(_ > 0))
	//		//			.foreach(println)
	//	}
	//
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
