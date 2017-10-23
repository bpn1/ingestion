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
import de.hpi.ingestion.deduplication.blockingschemes.{ListBlockingScheme, SimpleBlockingScheme}
import de.hpi.ingestion.deduplication.models.Block
import de.hpi.ingestion.framework.CommandLineConf
import org.scalatest.{FlatSpec, Matchers}

class BlockingTest extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {

	"Blocking" should "partition subjects regarding the value of the given key" in {
		val subjects = TestData.subjects
		val subjectRDD = sc.parallelize(subjects)
		val stagedSubjects = TestData.stagings
		val stagingRDD = sc.parallelize(stagedSubjects)
		val blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		val subjectBlocks = Blocking.blockSubjects(subjectRDD, blockingSchemes)
		val stagingBlocks = Blocking.blockSubjects(stagingRDD, blockingSchemes)
		val expectedSubjects = TestData.flatSubjectBlocks(sc, subjects)
		val expectedStaging = TestData.flatStagingBlocks(sc, stagedSubjects)
		assertRDDEquals(subjectBlocks, expectedSubjects)
		assertRDDEquals(stagingBlocks, expectedStaging)
	}

	it should "group subjects by their key and blockSubjects scheme" in {
		val job = new Blocking
		job.maxBlockSize = job.settings("maxBlockSize").toInt
		val subjects = TestData.subjects
		job.subjects = sc.parallelize(subjects)
		val stagedSubjects = TestData.stagings
		job.stagedSubjects = sc.parallelize(stagedSubjects)
		job.blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		job.filterSmall = false
		job.filterUndefined = false
		val blocks = job.blocking().map { case (tag, block) => (tag, block.copy(id = null)) }
		val expectedBlocks = TestData.groupedBlocks(sc, subjects, stagedSubjects)
		assertRDDEquals(blocks, expectedBlocks)
	}

	it should "filter undefined blocks" in {
		val job = new Blocking
		job.maxBlockSize = job.settings("maxBlockSize").toInt
		val subjects = TestData.subjects
		job.subjects = sc.parallelize(subjects)
		val stagedSubjects = TestData.stagings
		job.stagedSubjects = sc.parallelize(stagedSubjects)
		job.blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		job.filterSmall = false
		job.filterUndefined = true
		val blocks = job.blocking().map { case (tag, block) => (tag, block.copy(id = null)) }
		val expectedBlocks = TestData.groupedAndFilteredBlocks(sc, subjects, stagedSubjects)
		assertRDDEquals(blocks, expectedBlocks)
	}

	it should "split blocks if they are too large" in {
		val job = new Blocking
		job.subjects = sc.parallelize(TestData.blockSplitSubjects())
		job.stagedSubjects = sc.parallelize(TestData.blockSplitStagedSubjects())
		job.blockingSchemes = List(SimpleBlockingScheme("SimpleBlocking"))
		job.filterSmall = true
		job.filterUndefined = true
		job.maxBlockSize = 40L
		val blocks = job.blocking()
			.values
			.map(_.copy(id = null))
			.collect
			.toSet
		val expectedBlocks = TestData.splitBlocks()
		blocks shouldEqual expectedBlocks
	}

	"Block evaluation" should "create evaluation blocks" in {
		val job = new Blocking
		val subjects = TestData.subjects
		job.subjects = sc.parallelize(subjects)
		val stagedSubjects = TestData.stagings
		job.stagedSubjects = sc.parallelize(stagedSubjects)
		val goldStandard = TestData.goldStandard(subjects, stagedSubjects)
		val comment = "Test comment"
		job.blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		job.filterUndefined = false
		job.filterSmall = false
		job.calculatePC = true
		job.evaluationBlocking(goldStandard, comment)
		val evaluationBlocks = job.blockEvaluation
			.map(_.copy(jobid = null))
			.collect
			.toSet
		val expectedEvaluations = TestData.blockEvaluationWithComment()
		evaluationBlocks shouldEqual expectedEvaluations
	}

	it should "skip the pairs completeness calcuation" in {
		val job = new Blocking
		val subjects = TestData.subjects
		job.subjects = sc.parallelize(subjects)
		val stagedSubjects = TestData.stagings
		job.stagedSubjects = sc.parallelize(stagedSubjects)
		val goldStandard = TestData.goldStandard(subjects, stagedSubjects)
		val comment = "Test comment"
		job.blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		job.filterUndefined = false
		job.filterSmall = false
		job.evaluationBlocking(goldStandard, comment)
		val evaluationBlocks = job.blockEvaluation
			.map(_.copy(jobid = null))
			.collect
			.toSet
		val expectedEvaluations = TestData.blockEvaluationWithOutPC()
		evaluationBlocks shouldEqual expectedEvaluations
	}

	it should "filter undefined blocks" in {
		val job = new Blocking
		val subjects = TestData.subjects
		job.subjects = sc.parallelize(subjects)
		val stagedSubjects = TestData.stagings
		job.stagedSubjects = sc.parallelize(stagedSubjects)
		val goldStandard = TestData.goldStandard(subjects, stagedSubjects)
		val comment = "Blocking"
		job.blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		job.filterUndefined = true
		job.filterSmall = false
		job.calculatePC = true
		job.evaluationBlocking(goldStandard, comment)
		val evaluationBlocks = job.blockEvaluation
			.map(_.copy(jobid = null))
			.collect
			.toSet
		val expectedEvaluations = TestData.filteredUndefinedBlockEvaluation()
		evaluationBlocks shouldEqual expectedEvaluations
	}

	it should "filter empty blocks" in {
		val job = new Blocking
		val (subjects, stagedSubjects) = TestData.blockEvaluationTestSubjects()
		job.subjects = sc.parallelize(subjects)
		job.stagedSubjects = sc.parallelize(stagedSubjects)
		val goldStandard = TestData.goldStandard(TestData.subjects, TestData.stagings)
		val comment = "Blocking"
		job.blockingSchemes = List(SimpleBlockingScheme("SimpleBlocking"))
		job.filterUndefined = true
		job.filterSmall = true
		job.calculatePC = true
		job.evaluationBlocking(goldStandard, comment)
		val evaluationBlocks = job.blockEvaluation
			.map(_.copy(jobid = null))
			.collect
			.toSet
		val expectedEvaluations = TestData.filteredSmallBlockEvaluation()
		evaluationBlocks shouldEqual expectedEvaluations
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
		job.calculatePC = true
		job.run(sc)
		val evaluationBlocks = job.blockEvaluation
			.map(_.copy(jobid = null, data = Set()))
			.collect
			.toSet
		val expectedEvaluations = TestData.emptyBlockEvaluation()
		evaluationBlocks shouldEqual expectedEvaluations
	}

	it should "set the comment if given the config option" in {
		val job = new Blocking
		val subjects = TestData.subjects
		val stagedSubjects = TestData.stagings
		job.subjects = sc.parallelize(subjects)
		job.stagedSubjects = sc.parallelize(stagedSubjects)
		val goldStandard = TestData.goldStandard(subjects, stagedSubjects)
		job.goldStandard = sc.parallelize(goldStandard.toList)
		job.setBlockingSchemes(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		job.conf = CommandLineConf(Seq("-b", "Test comment"))
		job.calculatePC = true
		job.run(sc)
		val evaluationBlocks = job.blockEvaluation
			.map(_.copy(jobid = null, data = Set()))
			.collect
			.toSet
		val expectedEvaluations = TestData.emptyBlockEvaluationWithComment()
		evaluationBlocks shouldEqual expectedEvaluations
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
		val stagedSubjects = TestData.stagings
		val duplicates = TestData.duplicateStats()
		val duplicatesRDD = sc.parallelize(duplicates)
		val goldStandard = TestData.goldStandard(subjects, stagedSubjects)
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
		val stagedSubjects = TestData.stagings
		val block = Block(key = "key", subjects = subjects, staging = stagedSubjects)
		val goldStandard = TestData.goldStandard(subjects, stagedSubjects)
		val duplicateStats = Blocking.createDuplicateStats(("tag", block), goldStandard)
		val expected = TestData.smallerDuplicateStats
		duplicateStats shouldEqual expected
	}

	"Member values" should "be set" in {
		val job = new Blocking
		val maxBlockSize = job.maxBlockSize
		job.setMaxBlockSize(List(1, 2, 3))
		job.maxBlockSize shouldEqual maxBlockSize
		job.setMaxBlockSize(123)
		job.maxBlockSize shouldEqual 123L
		job.setMaxBlockSize(234L)
		job.maxBlockSize shouldEqual 234L
		job.setMaxBlockSize("567")
		job.maxBlockSize shouldEqual 567L

		val minBlockSize = job.minBlockSize
		job.setMinBlockSize(List(1, 2, 3))
		job.minBlockSize shouldEqual minBlockSize
		job.setMinBlockSize(123)
		job.minBlockSize shouldEqual 123L
		job.setMinBlockSize(234L)
		job.minBlockSize shouldEqual 234L
		job.setMinBlockSize("567")
		job.minBlockSize shouldEqual 567L
	}

	"Blocks" should "be merged with the subset Blocks" in {
		val job = new Blocking
		job.blockingSchemes = List(SimpleBlockingScheme("tag"))
		val blocksWithSubsets = sc.parallelize(TestData.blocksWithSubsetBlocks("tag"))
		val mergedBlocks = job
			.mergeSubsetBlocks(blocksWithSubsets)
			.values
			.collect
			.map(block => (block.key, block))
			.toMap
		mergedBlocks.keySet should contain ("audi")
		mergedBlocks.keySet should contain ("au")
		mergedBlocks.keySet should contain ("audi ")
		mergedBlocks.get("audi ").foreach { block =>
			block.subjects should have length 13
			block.staging should have length 10
		}
		mergedBlocks.keySet should contain ("bmw")
		mergedBlocks.keySet should contain ("bmw g")
		mergedBlocks.get("bmw g").foreach { block =>
			block.subjects should have length 3
			block.staging should have length 30
		}
		mergedBlocks.keySet should contain ("volks")
		mergedBlocks.get("volks").foreach { block =>
			block.subjects should have length 4
			block.staging should have length 7
		}
		mergedBlocks.keySet should contain ("vw")
		mergedBlocks.get("vw").foreach { block =>
			block.subjects should have length 6
			block.staging should have length 8
		}
	}

	they should "not be merged without a Simple Blocking Scheme" in {
		val job = new Blocking
		job.blockingSchemes = List(ListBlockingScheme("tag", "geo_postal"))
		val blocksWithSubsets = TestData.blocksWithSubsetBlocks("tag")
		val mergedBlocks = job
			.mergeSubsetBlocks(sc.parallelize(blocksWithSubsets))
			.collect
			.toSet
		blocksWithSubsets.toSet shouldEqual mergedBlocks
	}

	they should "only be merged if they are from a Simple Blocking Scheme" in {
		val job = new Blocking
		job.blockingSchemes = List(SimpleBlockingScheme("tag1"), ListBlockingScheme("tag2", "geo_postal"))
		val blocksWithSubsets = sc.parallelize(TestData.blocksWithSubsetBlocksAndTags("tag1", "tag2"))
		val mergedBlocks = job
			.mergeSubsetBlocks(blocksWithSubsets)
			.collect
			.map { case (tag, block) => (s"${tag}_${block.key}", block) }
			.toMap
		mergedBlocks.keySet should contain ("tag1_audi ")
		mergedBlocks.keySet should contain ("tag2_audi ")
		mergedBlocks.get("tag1_audi ").foreach { block =>
			block.subjects should have length 13
			block.staging should have length 10
		}
		mergedBlocks.get("tag2_audi ").foreach { block =>
			block.subjects should have length 5
			block.staging should have length 4
		}
		mergedBlocks.keySet should contain ("tag1_bmw g")
		mergedBlocks.keySet should contain ("tag2_bmw g")
		mergedBlocks.get("tag1_bmw g").foreach { block =>
			block.subjects should have length 3
			block.staging should have length 30
		}
		mergedBlocks.get("tag2_bmw g").foreach { block =>
			block.subjects should have length 2
			block.staging should have length 20
		}
	}

	they should "be merged if they are empty and would create a non empty Block" in {
		val job = new Blocking
		job.blockingSchemes = List(SimpleBlockingScheme("tag1"))
		val (subjects, stagedSubjects) = TestData.subjectsForEmptyBlocks()
		job.subjects = sc.parallelize(subjects)
		job.stagedSubjects = sc.parallelize(stagedSubjects)
		val blocks = job
			.blocking()
			.values
			.collect
			.toList
		blocks should not be empty
		val audiBlock = blocks.find(_.key == "audi ").get
		audiBlock.numComparisons shouldEqual 2
		audiBlock.subjects should have length 2
		audiBlock.staging should have length 1
	}
}
