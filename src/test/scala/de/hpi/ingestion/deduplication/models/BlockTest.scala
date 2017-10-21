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

package de.hpi.ingestion.deduplication.models

import de.hpi.ingestion.datalake.models.Subject
import org.scalatest.{FlatSpec, Matchers}

class BlockTest extends FlatSpec with Matchers {
	"Number of comparisons" should "be calculated" in {
		val subjects = List.fill(5)(Subject.master())
		val staging = List.fill(3)(Subject.master())
		val block = Block(key = "test", subjects = subjects, staging = staging)
		block.numComparisons shouldEqual 15
	}

	"Cross product" should "be created" in {
		val blocks = TestData.crossBlocks()
		val crossProducts = blocks
			.flatMap(_.crossProduct())
			.toSet
		val expectedCrossProducts = TestData.crossedSubjects()
		crossProducts shouldEqual expectedCrossProducts
	}

	it should "apply a filter function" in {
		val blocks = TestData.crossBlocks()
		val crossProducts = blocks
			.flatMap(_.crossProduct((s1: Subject, s2: Subject) => s1.id.toString.substring(0, 1).toInt < 4))
			.toSet
		val expectedCrossProducts = TestData.filteredCrossedSubjects()
		crossProducts shouldEqual expectedCrossProducts
	}

	"Large Blocks" should "be divided into smaller blocks correctly" in {
		val splitBlocks = TestData.largeBlocks()
			.map((Block.split _).tupled)
			.map(_.map(_.copy(id = null)))
		val expectedSplits = TestData.splitBlocks()
		splitBlocks shouldEqual expectedSplits
	}

}
