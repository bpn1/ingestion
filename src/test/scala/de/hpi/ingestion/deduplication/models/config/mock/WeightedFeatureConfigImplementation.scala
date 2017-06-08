package de.hpi.ingestion.deduplication.models.config.mock

import de.hpi.ingestion.deduplication.models.config.WeightedFeatureConfig

/**
  * Mock Object for testing the WeightedFeatureConfig Trait
  */
case class WeightedFeatureConfigImplementation(
	weight: Double = 0.0
) extends WeightedFeatureConfig {
	type T = WeightedFeatureConfigImplementation
	override def updateWeight(weight: Double): WeightedFeatureConfigImplementation = {
		this.copy(weight = weight)
	}
}
