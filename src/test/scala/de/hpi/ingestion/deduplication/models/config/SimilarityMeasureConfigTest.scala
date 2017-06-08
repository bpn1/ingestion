package de.hpi.ingestion.deduplication.models.config

import de.hpi.ingestion.deduplication.similarity.{JaroWinkler, SimilarityMeasure}
import org.scalatest.{FlatSpec, Matchers}

class SimilarityMeasureConfigTest extends FlatSpec with Matchers {
	"updateWeight" should "update the weight" in {
		val config = SimilarityMeasureConfig[String, SimilarityMeasure[String]](JaroWinkler, 1.0)
		val updated = config.updateWeight(0.5).weight
		updated shouldEqual 0.5
	}

	"compare" should "calculate the similarity of two given objects applying the weight" in {
		val config = SimilarityMeasureConfig[String, SimilarityMeasure[String]](JaroWinkler, 0.5)
		val (left, right) = ("Christian", "Christine")
		val score = config.compare(left, right)
		val expected = JaroWinkler.compare(left, right) * 0.5
		score shouldEqual expected
	}
}
