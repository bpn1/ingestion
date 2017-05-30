package de.hpi.ingestion.deduplication.similarity

import org.scalatest.{FlatSpec, Matchers}

class DiceSorensenUnitTest extends FlatSpec with Matchers {

	"compare" should "return the DiceSorensen score for given strings" in {
		val testData = List(
			("night", "nachts", 0.5454545454545454),
			("context", "contact", 0.7142857142857143))

		testData.foreach(tuple =>
			DiceSorensen.compare(tuple._1, tuple._2) shouldEqual tuple._3)
	}
}
