package de.hpi.ingestion.deduplication.models.config

import org.scalatest.{FlatSpec, Matchers}

class WeightedFeatureConfigTest extends FlatSpec with Matchers {
	"normalizeWeights" should "normalize the weights of given list" in {
		val config = TestData.config
		val expected = TestData.normalizedConfigs
		(config, expected).zipped.foreach { case (config, expected) =>
			WeightedFeatureConfig.normalizeWeights(config) shouldEqual expected
		}
	}

	it should "discard all 0.0 weighted features" in {
		val config = TestData.incompleteConfig
		val expected = TestData.normalizedIncompleteConfig
		(config, expected).zipped.foreach { case (config, expected) =>
			WeightedFeatureConfig.normalizeWeights(config)  shouldEqual expected
		}
	}

	it should "fill default weights if none given" in {
		val config = TestData.zeroConfig
		val expected = TestData.normalizedZeroConfig
		(config, expected).zipped.foreach { case (config, expected) =>
			WeightedFeatureConfig.normalizeWeights(config)  shouldEqual expected
		}
	}
}
