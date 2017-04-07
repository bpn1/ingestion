package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class WikipediaTfidfCalculatorTest extends FlatSpec with SharedSparkContext with Matchers {
	"Bags of words for articles" should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		WikipediaTfidfCalculator.articlesToBagsOfWords(articles)
			.map(_.nonEmpty)
			.collect
			.foreach(bag => bag should not be empty)
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
