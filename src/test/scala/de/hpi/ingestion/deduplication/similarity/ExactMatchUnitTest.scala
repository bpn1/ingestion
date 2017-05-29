package de.hpi.ingestion.deduplication.similarity

import org.scalatest.{FlatSpec, Matchers}

class ExactMatchUnitTest extends FlatSpec with Matchers {

	"compare" should "return 1.0 or 0.0 for given strings" in {
		val testData = List(
			("tags", "nachts", 0.0),
			("context", "context", 1.0))

		testData.foreach(tuple =>
			ExactMatchString.compare(tuple._1, tuple._2) shouldEqual tuple._3)
	}

	it should "return 1.0 or 0.0 for given doubles" in {
		val testData = List(
			(0.3, 0.3, 1.0),
			(0.2, 0.4, 0.0))

		testData.foreach(tuple =>
			ExactMatchDouble.compare(tuple._1, tuple._2) shouldEqual tuple._3)
	}
}
