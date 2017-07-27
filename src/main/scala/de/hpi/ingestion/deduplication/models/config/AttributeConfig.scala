/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
