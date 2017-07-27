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
