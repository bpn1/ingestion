package de.hpi.ingestion.versioncontrol

import java.util.UUID

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.scalatest.{FlatSpec, Matchers}

class VersionRestoreTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {

    val restoreVersion = UUID.fromString("fc2c5a40-c566-11e6-aee2-5f2c06e3b302")

	"Subject data" should "be restored" in {
		val subjects = sc.parallelize(TestData.diffSubjects())
		val versions = Array(TestData.versionsToCompare()._2.toString)
		val rddList = List(subjects).toAnyRDD()
		val restoredSubjects = VersionRestore.run(rddList, sc, versions).fromAnyRDD[Subject]().head
		val expectedRestoredSubjects = sc.parallelize(TestData.restoredSubjects())
		assertRDDEquals(restoredSubjects, expectedRestoredSubjects)
	}
}
