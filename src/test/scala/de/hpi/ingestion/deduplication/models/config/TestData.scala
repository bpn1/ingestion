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

import de.hpi.ingestion.deduplication.models.config.mock.WeightedFeatureConfigImplementation
import de.hpi.ingestion.deduplication.similarity.{JaroWinkler, MongeElkan}

// scalastyle:off line.size.limit
object TestData {
	def config: List[List[WeightedFeatureConfigImplementation]] = List(
		List(WeightedFeatureConfigImplementation(0.5), WeightedFeatureConfigImplementation(0.3)),
		List(WeightedFeatureConfigImplementation(1.2), WeightedFeatureConfigImplementation(0.7))
	)

	def normalizedConfigs: List[List[WeightedFeatureConfigImplementation]] = List(
		List(WeightedFeatureConfigImplementation(0.625), WeightedFeatureConfigImplementation(0.375)),
		List(WeightedFeatureConfigImplementation(0.6316), WeightedFeatureConfigImplementation(0.3684))
	)

	def incompleteConfig: List[List[WeightedFeatureConfigImplementation]] = List(
		List(WeightedFeatureConfigImplementation(0.8), WeightedFeatureConfigImplementation()),
		List(WeightedFeatureConfigImplementation(0.3), WeightedFeatureConfigImplementation(0.8), WeightedFeatureConfigImplementation())
	)

	def normalizedIncompleteConfig: List[List[WeightedFeatureConfigImplementation]] = List(
		List(WeightedFeatureConfigImplementation(1.0)),
		List(WeightedFeatureConfigImplementation(0.2727), WeightedFeatureConfigImplementation(0.7273))
	)

	def zeroConfig: List[List[WeightedFeatureConfigImplementation]] = List(
		List(WeightedFeatureConfigImplementation(), WeightedFeatureConfigImplementation()),
		List(WeightedFeatureConfigImplementation(), WeightedFeatureConfigImplementation(), WeightedFeatureConfigImplementation())
	)

	def normalizedZeroConfig: List[List[WeightedFeatureConfigImplementation]] = List(
		List(WeightedFeatureConfigImplementation(0.5), WeightedFeatureConfigImplementation(0.5)),
		List(WeightedFeatureConfigImplementation(0.3333), WeightedFeatureConfigImplementation(0.3333), WeightedFeatureConfigImplementation(0.3333))
	)

	def attributeConfig: List[AttributeConfig] = List(
		AttributeConfig("a1", 1, List(SimilarityMeasureConfig(JaroWinkler, 0.3), SimilarityMeasureConfig(MongeElkan, 0.3))),
		AttributeConfig("a2", 0.5, List(SimilarityMeasureConfig(JaroWinkler, 5.0), SimilarityMeasureConfig(MongeElkan, 3.0))),
		AttributeConfig("a3", 0.25, List(SimilarityMeasureConfig(JaroWinkler))),
		AttributeConfig("a4", 0.25, List(SimilarityMeasureConfig(JaroWinkler, 0.5), SimilarityMeasureConfig(MongeElkan)))
	)

	def normalizedAttributeConfig: List[AttributeConfig] = List(
		AttributeConfig("a1", 0.5, List(SimilarityMeasureConfig(JaroWinkler, 0.5), SimilarityMeasureConfig(MongeElkan, 0.5))),
		AttributeConfig("a2", 0.25, List(SimilarityMeasureConfig(JaroWinkler, 0.625), SimilarityMeasureConfig(MongeElkan, 0.375))),
		AttributeConfig("a3", 0.125, List(SimilarityMeasureConfig(JaroWinkler, 1.0))),
		AttributeConfig("a4", 0.125, List(SimilarityMeasureConfig(JaroWinkler, 1.0)))
	)
}
// scalastyle:on line.size.limit
