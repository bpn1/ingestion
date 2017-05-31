package de.hpi.ingestion.deduplication

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.deduplication.blockingschemes.SimpleBlockingScheme
import de.hpi.ingestion.deduplication.models.BlockEvaluation
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

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
		val subjects = TestData.subjects
		val subjectRDD = sc.parallelize(subjects)
		val staging = TestData.stagings
		val stagingRDD = sc.parallelize(staging)
		val blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		val blocks = Blocking.blocking(subjectRDD, stagingRDD, blockingSchemes, false)
			.map { case (tag, block) => (tag, block.copy(id = null)) }
		val expectedBlocks = TestData.groupedBlocks(sc, subjects, staging)
		assertRDDEquals(blocks, expectedBlocks)
	}

	it should "filter undefined blocks" in {
		val subjects = TestData.subjects
		val subjectRDD = sc.parallelize(subjects)
		val staging = TestData.stagings
		val stagingRDD = sc.parallelize(staging)
		val blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		val blocks = Blocking.blocking(subjectRDD, stagingRDD, blockingSchemes, true)
			.map { case (tag, block) => (tag, block.copy(id = null)) }
		val expectedBlocks = TestData.groupedAndFilteredBlocks(sc, subjects, staging)
		assertRDDEquals(blocks, expectedBlocks)
	}

	"Block evaluation" should "create evaluation blocks" in {
		val subjects = TestData.subjects
		val subjectRDD = sc.parallelize(subjects)
		val staging = TestData.stagings
		val stagingRDD = sc.parallelize(staging)
		val goldStandard = TestData.goldStandard(subjects, staging)
		val comment = "Test comment"
		val blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		val evaluationBlocks = Blocking.evaluationBlocking(
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
		val subjects = TestData.subjects
		val subjectRDD = sc.parallelize(subjects)
		val staging = TestData.stagings
		val stagingRDD = sc.parallelize(staging)
		val goldStandard = TestData.goldStandard(subjects, staging)
		val comment = "Blocking"
		val blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		val evaluationBlocks = Blocking.evaluationBlocking(
			subjectRDD,
			stagingRDD,
			goldStandard,
			blockingSchemes,
			true,
			false,
			comment
		).map(_.copy(jobid = null))
		val expected = TestData.filteredBlockEvaluation(sc)
		assertRDDEquals(evaluationBlocks, expected)
	}

	"Blocking job" should "evaluate blocks" in {
		val subjects = TestData.subjects
		val subjectRDD = sc.parallelize(subjects)
		val staging = TestData.stagings
		val stagingRDD = sc.parallelize(staging)
		val goldStandard = TestData.goldStandard(subjects, staging)
		val goldStandardRDD = sc.parallelize(goldStandard.toList)
		val input = List(subjectRDD, stagingRDD).toAnyRDD() ::: List(goldStandardRDD).toAnyRDD()
		Blocking.setBlockingSchemes(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		val evaluationBlocks = Blocking.run(input, sc).fromAnyRDD[BlockEvaluation]().head
			.map(_.copy(jobid = null))
		val expected = TestData.blockEvaluation(sc)
		assertRDDEquals(evaluationBlocks, expected)
	}

	it should "take the first program argument as comment" in {
		val subjects = TestData.subjects
		val subjectRDD = sc.parallelize(subjects)
		val staging = TestData.stagings
		val stagingRDD = sc.parallelize(staging)
		val goldStandard = TestData.goldStandard(subjects, staging)
		val goldStandardRDD = sc.parallelize(goldStandard.toList)
		val input = List(subjectRDD, stagingRDD).toAnyRDD() ::: List(goldStandardRDD).toAnyRDD()
		Blocking.setBlockingSchemes(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		val evaluationBlocks = Blocking
			.run(input, sc, Array("Test comment"))
			.fromAnyRDD[BlockEvaluation]().head
			.map(_.copy(jobid = null))
		val expected = TestData.blockEvaluationWithComment(sc)
		assertRDDEquals(evaluationBlocks, expected)
	}

	it should "use the Simple Blocking Scheme if there is no other scheme defined" in {
		val originalSchemes = Blocking.blockingSchemes

		Blocking.blockingSchemes should not be empty
		Blocking.setBlockingSchemes(Nil)
		Blocking.blockingSchemes should not be empty
		Blocking.blockingSchemes should have length 1
		Blocking.blockingSchemes.head.isInstanceOf[SimpleBlockingScheme] shouldBe true

		Blocking.setBlockingSchemes(originalSchemes)
	}

	"Config" should "be read" in {
		val originalSettings = Blocking.settings(false)

		Blocking.configFile = "test.xml"
		Blocking.settings should not be empty

		Blocking.settings = originalSettings
	}

	it should "be read before run is executed" in {
		val originalSettings = Blocking.settings(false)

		Blocking.assertConditions(Array[String]())
		Blocking.settings should not be empty

		Blocking.settings = originalSettings
	}

	"createDuplicateStats" should "find all actual duplicates in a block" in {
		val subjects = TestData.subjects
		val stagings = TestData.stagings
		val UUIDSubjects = subjects.map(_.id)
		val UUIDStagings = stagings.map(_.id)
		val goldStandard = TestData.goldStandard(subjects, stagings)
		val duplicateStats = Blocking.createDuplicateStats((("tag", "key"),(UUIDSubjects, UUIDStagings)), goldStandard)
		val expected = TestData.testDuplicateStats
		duplicateStats shouldEqual expected
	}
}
