package de.hpi.ingestion.deduplication

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by radscheit on 04.05.17.
  */
class BlockingUnitTest extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {
	"blocking" should "partition subjects regarding the value of the given key" in {
		val subjects = TestData.testSubjects
		val subjectsRDD = sc.parallelize(subjects)
		val blockingSchemes = List(TestData.cityBlockingScheme, new SimpleBlockingScheme)
		val blocks = BlockingJob.blocking(subjectsRDD, blockingSchemes)
		val expectedBlocks = TestData.blocks(sc, subjects)

		assertRDDEquals(expectedBlocks, blocks)
	}

	"evaluateBlocks" should "evaluate each block" in {
		val subjects = TestData.testSubjects
		val blocks = TestData.blocks(sc, subjects)
		val blockingSchemes = List(TestData.cityBlockingScheme, new SimpleBlockingScheme)
		val evaluationList = BlockingJob.evaluateBlocks(blocks, "Test comment")
		val evaluationData = evaluationList
			.map(blockingData => (blockingData._2, blockingData._3))
			.collect
			.toSet
		val expected = TestData.evaluationTestData.toSet
		evaluationData shouldEqual expected
	}
}
