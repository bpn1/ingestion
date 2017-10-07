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

package de.hpi.ingestion.deduplication

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.deduplication.blockingschemes.SimpleBlockingScheme
import de.hpi.ingestion.deduplication.models.BlockEvaluation
import org.scalatest.{FlatSpec, Matchers}

class BlockingUnitTest extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {

	"Blocking" should "partition subjects regarding the value of the given key" in {
		val subjects = TestData.subjects
		val subjectRDD = sc.parallelize(subjects)
		val staging = TestData.stagings
		val stagingRDD = sc.parallelize(staging)
		val blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		val subjectBlocks = Blocking.blocking(subjectRDD, blockingSchemes)
		val stagingBlocks = Blocking.blocking(stagingRDD, blockingSchemes)
		val expectedSubjects = TestData.flatSubjectBlocks(sc, subjects)
		val expectedStaging = TestData.flatStagingBlocks(sc, staging)
		assertRDDEquals(subjectBlocks, expectedSubjects)
		assertRDDEquals(stagingBlocks, expectedStaging)
	}

	it should "group subjects by their key and blocking scheme" in {
		val job = new Blocking
		val maxBlockSize = job.settings("maxBlockSize").toInt
		val subjects = TestData.subjects
		val subjectRDD = sc.parallelize(subjects)
		val staging = TestData.stagings
		val stagingRDD = sc.parallelize(staging)
		val blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		val blocks = Blocking.blocking(subjectRDD, stagingRDD, blockingSchemes, maxBlockSize, false)
			.map { case (tag, block) => (tag, block.copy(id = null)) }
		val expectedBlocks = TestData.groupedBlocks(sc, subjects, staging)
		assertRDDEquals(blocks, expectedBlocks)
	}

	it should "filter undefined blocks" in {
		val job = new Blocking
		val maxBlockSize = job.settings("maxBlockSize").toInt
		val subjects = TestData.subjects
		val subjectRDD = sc.parallelize(subjects)
		val staging = TestData.stagings
		val stagingRDD = sc.parallelize(staging)
		val blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		val blocks = Blocking.blocking(subjectRDD, stagingRDD, blockingSchemes, maxBlockSize, true)
			.map { case (tag, block) => (tag, block.copy(id = null)) }
		val expectedBlocks = TestData.groupedAndFilteredBlocks(sc, subjects, staging)
		assertRDDEquals(blocks, expectedBlocks)
	}

	"Block evaluation" should "create evaluation blocks" in {
		val job = new Blocking
		val subjects = TestData.subjects
		val subjectRDD = sc.parallelize(subjects)
		val staging = TestData.stagings
		val stagingRDD = sc.parallelize(staging)
		val goldStandard = TestData.goldStandard(subjects, staging)
		val comment = "Test comment"
		val blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		val evaluationBlocks = job.evaluationBlocking(
			subjectRDD,
			stagingRDD,
			goldStandard,
			blockingSchemes,
			false,
			false,
			comment
		).map(_.copy(jobid = null))
		val expected = TestData.blockEvaluationWithComment(sc)
		assertRDDEquals(evaluationBlocks, expected)
	}

	it should "filter undefined blocks" in {
		val job = new Blocking
		val subjects = TestData.subjects
		val subjectRDD = sc.parallelize(subjects)
		val staging = TestData.stagings
		val stagingRDD = sc.parallelize(staging)
		val goldStandard = TestData.goldStandard(subjects, staging)
		val comment = "Blocking"
		val blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		val evaluationBlocks = job.evaluationBlocking(
			subjectRDD,
			stagingRDD,
			goldStandard,
			blockingSchemes,
			true,
			false,
			comment
		).map(_.copy(jobid = null))
		val expected = TestData.filteredUndefinedBlockEvaluation(sc)
		assertRDDEquals(evaluationBlocks, expected)
	}

	it should "filter small, but not filter medium blocks" in {
		val job = new Blocking
		val (subjectRDD, stagingRDD) = TestData.blockEvaluationTestSubjects(sc)
		val goldStandard = TestData.goldStandard(TestData.subjects, TestData.stagings)
		val comment = "Blocking"
		val blockingSchemes = List(SimpleBlockingScheme("SimpleBlocking"))
		val evaluationBlocks = job.evaluationBlocking(
			subjectRDD,
			stagingRDD,
			goldStandard,
			blockingSchemes,
			true,
			true,
			comment
		).map(_.copy(jobid = null))
		val expected = TestData.filteredSmallBlockEvaluation(sc)
		assertRDDEquals(evaluationBlocks, expected)
	}

	"Blocking job" should "evaluate blocks" in {
		val job = new Blocking
		val subjects = TestData.subjects
		val stagedSubjects = TestData.stagings
		job.subjects = sc.parallelize(subjects)
		job.stagedSubjects = sc.parallelize(stagedSubjects)
		val goldStandard = TestData.goldStandard(subjects, stagedSubjects)
		job.goldStandard = sc.parallelize(goldStandard.toList)
		job.setBlockingSchemes(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		job.run(sc)
		val evaluationBlocks = job.blockEvaluation.map(_.copy(jobid = null))
		val expected = TestData.emptyBlockEvaluation(sc)
		assertRDDEquals(evaluationBlocks, expected)
	}

	it should "take the first program argument as comment" in {
		val job = new Blocking
		val subjects = TestData.subjects
		val stagedSubjects = TestData.stagings
		job.subjects = sc.parallelize(subjects)
		job.stagedSubjects = sc.parallelize(stagedSubjects)
		val goldStandard = TestData.goldStandard(subjects, stagedSubjects)
		job.goldStandard = sc.parallelize(goldStandard.toList)
		job.setBlockingSchemes(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		job.args = Array("Test comment")
		job.run(sc)
		val evaluationBlocks = job.blockEvaluation.map(_.copy(jobid = null))
		val expected = TestData.emptyBlockEvaluationWithComment(sc)
		assertRDDEquals(evaluationBlocks, expected)
	}

	it should "use the Simple Blocking Scheme if there is no other scheme defined" in {
		val job = new Blocking
		job.blockingSchemes should not be empty
		job.setBlockingSchemes(Nil)
		job.blockingSchemes should not be empty
		job.blockingSchemes should have length 1
		job.blockingSchemes.head.isInstanceOf[SimpleBlockingScheme] shouldBe true
	}

//	"Config" should "be read" in {
//		val job = new Blocking
//		job.configFile = "test.xml"
//		job.settings should not be empty
//	}
// TODO: check if duplicate test
//	it should "be read before run is executed" in {
//		val job = new Blocking
//		job.assertConditions()
//		job.settings should not be empty
//	}

	"calculatePairwiseCompleteness" should "calculate all pairwise completenesses (PCs) as well as the total PC" in {
		val job = new Blocking
		val subjects = TestData.subjects
		val stagings = TestData.stagings
		val duplicates = TestData.testDuplicateStatsRDD(sc)
		val goldStandard = TestData.goldStandard(subjects, stagings)
		val sumTag = "sum"
		val pairwiseCompleteness = job.calculatePairsCompleteness(duplicates, goldStandard, sumTag)
		val expected = TestData.expectedPairwiseCompleteness
		pairwiseCompleteness shouldEqual expected
	}

	"calculateBlockCount" should "count all blocks for all blocking schemes as well as the total blocks" in {
		val job = new Blocking
		val blocks = TestData.blocks(sc)
		val sumTag = "sum"
		val blockCount = job.calculateBlockCount(blocks, sumTag)
		val expected = TestData.expectedBlockCount
		blockCount shouldEqual expected
	}

	"calculateCompareCount" should "count all compares in each blocking scheme as well as the total compares" in {
		val job = new Blocking
		val blocks = TestData.blocks(sc)
		val sumTag = "sum"
		val compareCount = job.calculateCompareCount(blocks, sumTag)
		val expected = TestData.expectedCompareCount
		compareCount shouldEqual expected
	}

	"createDuplicateStats" should "find all actual duplicates in a block" in {
		val job = new Blocking
		val subjects = TestData.subjects
		val stagings = TestData.stagings
		val UUIDSubjects = subjects.map(_.id)
		val UUIDStagings = stagings.map(_.id)
		val goldStandard = TestData.goldStandard(subjects, stagings)
		val duplicateStats = job.createDuplicateStats((("tag", "key"),(UUIDSubjects, UUIDStagings)), goldStandard)
		val expected = TestData.testDuplicateStats
		duplicateStats shouldEqual expected
	}
}
