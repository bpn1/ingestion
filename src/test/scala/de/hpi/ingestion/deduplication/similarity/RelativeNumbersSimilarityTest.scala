package de.hpi.ingestion.deduplication.similarity

import org.scalatest.{FlatSpec, Matchers}

class RelativeNumbersSimilarityTest extends FlatSpec with Matchers {
	"compare" should "calculate the similarity of two numbers" in {
		val computedScore1 = RelativeNumbersSimilarity.compare("1234", "12")
		val computedScore2 = RelativeNumbersSimilarity.compare("33", "600")
		val expectedScore1 = 12.0 / 1234.0
		val expectedScore2 = 33.0 / 600.0
		computedScore1 shouldEqual expectedScore1
		computedScore2 shouldEqual expectedScore2
	}
}
