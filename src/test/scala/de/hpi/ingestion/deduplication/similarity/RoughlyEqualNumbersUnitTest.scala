package de.hpi.ingestion.deduplication.similarity

import de.hpi.ingestion.deduplication.TestData
import org.scalatest.{FlatSpec, Matchers}

class RoughlyEqualNumbersUnitTest extends FlatSpec with Matchers {
	"compare" should "calculate the similarity of two numbers" in {
		val subjects = TestData.testSubjects
		val computedScore1 = RoughlyEqualNumbers.compare(
			subjects.head.properties("gen_income").head,
			subjects(1).properties("gen_income").head
		)
		val computedScore2 = RoughlyEqualNumbers.compare(
			subjects(2).properties("gen_income").head,
			subjects(3).properties("gen_income").head
		)
		val expectedScore1 = 12.0 / 1234.0
		val expectedScore2 = 33.0 / 600.0
		computedScore1 shouldEqual expectedScore1
		computedScore2 shouldEqual expectedScore2
	}
}
