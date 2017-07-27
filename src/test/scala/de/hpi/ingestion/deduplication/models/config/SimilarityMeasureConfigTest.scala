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
