package de.hpi.ingestion.datamerge

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.implicits.CollectionImplicits._

class MasterConnectingTest extends FlatSpec with Matchers with SharedSparkContext with RDDComparisons {
	"Master nodes" should "be connected" in {
		val subjects = sc.parallelize(TestData.mergedSubjects)
		val input = List(subjects).toAnyRDD()
		val output = MasterConnecting
			.run(input, sc)
			.fromAnyRDD[Subject]()
			.head
			.collect
			.sortBy(_.id)
			.map(_.relations)

		val expected = TestData
			.connectedMasters
			.sortBy(_.id)
			.map(_.relations)

		output.length shouldEqual expected.length
		(output, expected).zipped.foreach { case (relations, expectedRelations) =>
			relations shouldEqual expectedRelations
		}
	}
}
