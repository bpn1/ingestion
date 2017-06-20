package de.hpi.ingestion.versioncontrol

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.scalatest.{FlatSpec, Matchers}

class VersionRestoreTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {
	"Subject data" should "be restored" in {
		val subjects = sc.parallelize(TestData.diffSubjects())
		val versions = Array(TestData.versionsToCompare()._2.toString)
		val rddList = List(subjects).toAnyRDD()
		val restoredSubjects = VersionRestore.run(rddList, sc, versions)
			.fromAnyRDD[Subject]()
			.head
			.collect
			.sortBy(_.id)

		val zippedSubjects = restoredSubjects.zip(TestData.restoredSubjects())
		zippedSubjects.foreach { case (originalSubject, restoredSubject) =>
			originalSubject.name shouldEqual restoredSubject.name
			originalSubject.master shouldEqual restoredSubject.master
			originalSubject.aliases shouldEqual restoredSubject.aliases
			originalSubject.category shouldEqual restoredSubject.category
			originalSubject.properties shouldEqual restoredSubject.properties
			originalSubject.relations shouldEqual restoredSubject.relations
		}
	}
}
