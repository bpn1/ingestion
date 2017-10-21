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
import de.hpi.ingestion.deduplication.models.Block
import de.hpi.ingestion.framework.CommandLineConf
import org.scalatest.{FlatSpec, Matchers}

class BlockingTest extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {

	"Blocking" should "partition subjects regarding the value of the given key" in {
		val subjects = TestData.subjects
		val subjectRDD = sc.parallelize(subjects)
		val staging = TestData.stagings
		val stagingRDD = sc.parallelize(staging)
		val blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		val subjectBlocks = Blocking.blockSubjects(subjectRDD, blockingSchemes)
		val stagingBlocks = Blocking.blockSubjects(stagingRDD, blockingSchemes)
		val expectedSubjects = TestData.flatSubjectBlocks(sc, subjects)
		val expectedStaging = TestData.flatStagingBlocks(sc, staging)
		assertRDDEquals(subjectBlocks, expectedSubjects)
		assertRDDEquals(stagingBlocks, expectedStaging)
	}

	it should "group subjects by their key and blockSubjects scheme" in {
		val job = new Blocking
		job.maxBlockSize = job.settings("maxBlockSize").toInt
		val subjects = TestData.subjects
		job.subjects = sc.parallelize(subjects)
		val staging = TestData.stagings
		job.stagedSubjects = sc.parallelize(staging)
		job.blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		job.filterUndefined = false
		val blocks = job.blocking().map { case (tag, block) => (tag, block.copy(id = null)) }
		val expectedBlocks = TestData.groupedBlocks(sc, subjects, staging)
		assertRDDEquals(blocks, expectedBlocks)
	}

	it should "filter undefined blocks" in {
		val job = new Blocking
		job.maxBlockSize = job.settings("maxBlockSize").toInt
		val subjects = TestData.subjects
		job.subjects = sc.parallelize(subjects)
		val staging = TestData.stagings
		job.stagedSubjects = sc.parallelize(staging)
		job.blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		job.filterUndefined = true
		val blocks = job.blocking().map { case (tag, block) => (tag, block.copy(id = null)) }
		val expectedBlocks = TestData.groupedAndFilteredBlocks(sc, subjects, staging)
		assertRDDEquals(blocks, expectedBlocks)
	}

	"Block evaluation" should "create evaluation blocks" in {
		val job = new Blocking
		val subjects = TestData.subjects
		job.subjects = sc.parallelize(subjects)
		val staging = TestData.stagings
		job.stagedSubjects = sc.parallelize(staging)
		val goldStandard = TestData.goldStandard(subjects, staging)
		val comment = "Test comment"
		job.blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		job.evaluationBlocking(goldStandard, false, false, comment)
		val evaluationBlocks = job.blockEvaluation.map(_.copy(jobid = null))
		val expected = TestData.blockEvaluationWithComment(sc)
		assertRDDEquals(evaluationBlocks, expected)
	}

	it should "filter undefined blocks" in {
		val job = new Blocking
		val subjects = TestData.subjects
		job.subjects = sc.parallelize(subjects)
		val staging = TestData.stagings
		job.stagedSubjects = sc.parallelize(staging)
		val goldStandard = TestData.goldStandard(subjects, staging)
		val comment = "Blocking"
		job.blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		job.evaluationBlocking(goldStandard, true, false, comment)
		val evaluationBlocks = job.blockEvaluation.map(_.copy(jobid = null))
		val expected = TestData.filteredUndefinedBlockEvaluation(sc)
		assertRDDEquals(evaluationBlocks, expected)
	}

	it should "filter small, but not filter medium blocks" in {
		val job = new Blocking
		val (subjectRDD, stagingRDD) = TestData.blockEvaluationTestSubjects(sc)
		job.subjects = subjectRDD
		job. stagedSubjects = stagingRDD
		val goldStandard = TestData.goldStandard(TestData.subjects, TestData.stagings)
		val comment = "Blocking"
		job.blockingSchemes = List(SimpleBlockingScheme("SimpleBlocking"))
		job.evaluationBlocking(goldStandard, true, true, comment)
		val evaluationBlocks = job.blockEvaluation.map(_.copy(jobid = null))
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
		job.conf = CommandLineConf(Seq("-b", "Test comment"))
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

	"Pairs Completeness" should "be calculated for each Blocking Scheme and their combination" in {
		val subjects = TestData.subjects
		val stagings = TestData.stagings
		val duplicates = TestData.duplicateStats()
		val duplicatesRDD = sc.parallelize(duplicates)
		val goldStandard = TestData.goldStandard(subjects, stagings)
		val sumTag = "sum"
		val pairwiseCompleteness = Blocking.calculatePairsCompleteness(duplicatesRDD, goldStandard, sumTag)
		val expected = TestData.expectedPairwiseCompleteness
		pairwiseCompleteness shouldEqual expected
	}

	"Block Count" should "be calculated for each Blocking Scheme and their combination" in {
		val blocks = TestData.blocks()
		val blockRDD = sc.parallelize(blocks)
		val sumTag = "sum"
		val blockCount = Blocking.calculateBlockCount(blockRDD, sumTag)
		val expected = TestData.expectedBlockCount
		blockCount shouldEqual expected
	}

	"Comparison Count" should "be calculated for each Blocking Scheme and their combination" in {
		val blocks = TestData.blocks()
		val blockRDD = sc.parallelize(blocks)
		val sumTag = "sum"
		val compareCount = Blocking.calculateComparisonCount(blockRDD, sumTag)
		val expected = TestData.expectedCompareCount
		compareCount shouldEqual expected
	}

	"All Duplicates in a Block" should "be found" in {
		val subjects = TestData.subjects
		val stagings = TestData.stagings
		val block = Block(key = "key", subjects = subjects, staging = stagings)
		val goldStandard = TestData.goldStandard(subjects, stagings)
		val duplicateStats = Blocking.createDuplicateStats(("tag", block), goldStandard)
		val expected = TestData.testDuplicateStats
		duplicateStats shouldEqual expected
	}
}
