package de.hpi.ingestion.deduplication.models.config

import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure

/**
  * Reproduces a configuration for an attribute for deduplication.
  * @param attribute name of the attribute
  * @param weight weight
  * @param scoreConfigs List of ScoreConfigs
  */
case class AttributeConfig(
	attribute: String,
	weight: Double = 0.0,
	scoreConfigs: List[SimilarityMeasureConfig[String, SimilarityMeasure[String]]] = Nil
) extends WeightedFeatureConfig {
	type T = AttributeConfig
	override def updateWeight(weight: Double): AttributeConfig = this.copy(weight = weight)
}

/**
  * Companion Object for AttributeConfig case class
  */
object AttributeConfig {
	def normalizeWeights(configs: List[AttributeConfig]): List[AttributeConfig] = {
		WeightedFeatureConfig
			.normalizeWeights(configs)
			.map(config => config.copy(scoreConfigs = WeightedFeatureConfig.normalizeWeights(config.scoreConfigs)))
	}
}
