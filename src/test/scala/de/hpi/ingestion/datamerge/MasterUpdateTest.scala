package de.hpi.ingestion.datamerge

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.datalake.models.Subject
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class MasterUpdateTest extends FlatSpec with Matchers with SharedSparkContext {

	"Master nodes" should "be updated" in {
		val subjects = sc.parallelize(TestData.outdatedMasters())
		val input = List(subjects).toAnyRDD()
		val updatedMasters = MasterUpdate.run(input, sc).fromAnyRDD[Subject]().head.collect().toList.sortBy(_.id)
		val expectedMasters = TestData.updatedMasters().sortBy(_.id)
		updatedMasters should have length expectedMasters.length
		updatedMasters.zip(expectedMasters).foreach { case (master, expectedMaster) =>
			master.id shouldEqual expectedMaster.id
			master.name shouldEqual expectedMaster.name
			master.aliases shouldEqual expectedMaster.aliases
			master.category shouldEqual expectedMaster.category
			master.properties shouldEqual expectedMaster.properties
			master.relations shouldEqual expectedMaster.relations
		}
	}
}
