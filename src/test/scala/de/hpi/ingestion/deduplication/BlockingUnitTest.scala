package de.hpi.ingestion.deduplication

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.deduplication.blockingschemes.SimpleBlockingScheme
import org.scalatest.{FlatSpec, Matchers}

class BlockingUnitTest extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {

	"blocking" should "partition subjects regarding the value of the given key" in {
		val subjects = TestData.subjects
		val subjectRDD = sc.parallelize(subjects)
		val staging = TestData.stagings
		val stagingRDD = sc.parallelize(staging)
		val blockingSchemes = List(TestData.cityBlockingScheme, SimpleBlockingScheme("SimpleBlocking"))
		val subjectBlocks = BlockingJob.blocking(subjectRDD, blockingSchemes)
		val stagingBlocks = BlockingJob.blocking(stagingRDD, blockingSchemes)
		val blocks = subjectBlocks.union(stagingBlocks)
		val expectedBlocks = TestData.flatBlocks(sc, subjects, staging)
		assertRDDEquals(expectedBlocks, blocks)
	}

	"evaluateBlocks" should "evaluate each block" in {
		val blocks = TestData.blocks(sc)
		val evaluationList = BlockingJob.evaluateBlocks(blocks, "Test comment")
		val evaluationData = evaluationList
			.map(blockingData => (blockingData.comment, blockingData.data))
			.collect
			.toSet
		val expected = TestData.evaluationTestData.toSet
		evaluationData shouldEqual expected
	}
}
