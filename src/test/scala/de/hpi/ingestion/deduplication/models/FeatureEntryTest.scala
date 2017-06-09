package de.hpi.ingestion.deduplication.models

import org.scalatest.{FlatSpec, Matchers}

class FeatureEntryTest extends FlatSpec with Matchers {

	"Labeled points" should "be returned" in {
		val points = TestData.featureEntries().map(_.labeledPoint)
		val expected = TestData.labeledPoints()
		points shouldEqual expected
	}
}
