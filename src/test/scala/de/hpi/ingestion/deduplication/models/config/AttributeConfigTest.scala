package de.hpi.ingestion.deduplication.models.config

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Lando on 06.06.17.
  */
class AttributeConfigTest extends FlatSpec with Matchers {
	"updateWeight" should "update the weight" in {
		val config = AttributeConfig("name", 1.0)
		val updated = config.updateWeight(0.5)
		updated.weight shouldEqual 0.5
	}

	"normalizeWeights" should "normalize the weights of a given AttributeConfig list" in {
		val config = AttributeConfig.normalizeWeights(TestData.attributeConfig)
		val expected = TestData.normalizedAttributeConfig
		(config, expected).zipped.foreach { case (config, expected) =>
			config shouldEqual expected
		}
	}
}
