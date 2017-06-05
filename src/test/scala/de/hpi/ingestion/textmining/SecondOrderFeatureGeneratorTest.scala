package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class SecondOrderFeatureGeneratorTest extends FlatSpec with SharedSparkContext with Matchers {
	"Rank" should "be computed correctly" in {
		val values = TestData.valuesList()
		val ranks = SecondOrderFeatureGenerator.computeRanks(values)
		ranks shouldEqual TestData.ranksList()
	}

	it should "be computed correctly for a longer example" in {
		val values = TestData.longValuesList()
		val ranks = SecondOrderFeatureGenerator.computeRanks(values)
		ranks shouldEqual TestData.longRanksList()
	}

	"Difference to highest value (delta top)" should "be computed correctly" in {
		val values = TestData.valuesList()
		val deltaTops = SecondOrderFeatureGenerator.computeDeltaTopValues(values)
		deltaTops shouldEqual TestData.deltaTopValuesList()
	}

	"Difference to next value (delta successor)" should "be computed correctly" in {
		val values = TestData.valuesList()
		val deltaTops = SecondOrderFeatureGenerator.computeDeltaSuccessorValues(values)
		deltaTops shouldEqual TestData.deltaSuccessorValuesList()
	}
}
