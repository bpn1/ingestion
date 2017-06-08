package de.hpi.ingestion.deduplication.models.config

import scala.math.BigDecimal

/**
  * Weighted Feature Config Trait for every weighted feature of the deduplication config
  */
trait WeightedFeatureConfig {
	type T <: WeightedFeatureConfig
	def weight: Double
	def updateWeight(weight: Double): T
}

/**
  * Companion Object of WeightedFeatureConfig
  */
object WeightedFeatureConfig {
	def normalizeWeights[T <: WeightedFeatureConfig](configs: List[T]): List[T] = {
		val summedWeights = configs.map(_.weight).sum
		val roundWeight: Double => Double = weight => BigDecimal(weight)
			.setScale(4, BigDecimal.RoundingMode.HALF_UP)
			.toDouble
		if (summedWeights > 0) {
			configs
				.filter(_.weight > 0)
				.map(config => config.updateWeight(roundWeight(config.weight / summedWeights)).asInstanceOf[T])
		} else {
			val normalizedWeight = 1.0 / configs.size.toDouble
			configs.map(_.updateWeight(roundWeight(normalizedWeight)).asInstanceOf[T])
		}
	}
}
