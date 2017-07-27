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

package de.hpi.ingestion.deduplication.similarity

import org.scalatest.{FlatSpec, Matchers}

class RelativeNumbersSimilarityTest extends FlatSpec with Matchers {
	"compare" should "calculate the similarity of two numbers" in {
		val computedScore1 = RelativeNumbersSimilarity.compare("1234", "12")
		val computedScore2 = RelativeNumbersSimilarity.compare("33", "600")
		val expectedScore1 = 12.0 / 1234.0
		val expectedScore2 = 33.0 / 600.0
		computedScore1 shouldEqual expectedScore1
		computedScore2 shouldEqual expectedScore2
	}
}
