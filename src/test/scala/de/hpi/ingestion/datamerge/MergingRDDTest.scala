package de.hpi.ingestion.datamerge

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.FlatSpec

class MergingRDDTest extends FlatSpec with SharedSparkContext with RDDComparisons {
	"Merging" should "update return an update RDD for the subjects" in {
//		val input = TestData.inputRDD(sc)
//		val output = Merging.run(input, sc).head
//		val expected = TestData.outputRDD(sc).head
		//assertRDDEquals(expected, output)
	}
}
