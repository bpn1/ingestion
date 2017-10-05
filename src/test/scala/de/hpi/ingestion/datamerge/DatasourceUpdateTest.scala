package de.hpi.ingestion.datamerge

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.scalatest.{FlatSpec, Matchers}

class DatasourceUpdateTest extends FlatSpec with Matchers with SharedSparkContext {

	"Subjects" should "be updated" in {
		val oldSubjects = TestData.subjectsToUpdate()
		val newSubjects = TestData.updateSubjects()
		val version = TestData.version
		val updatedSubjects = oldSubjects.zip(newSubjects).map {
			case (oldSubject, newSubject) => DatasourceUpdate.updateSubject(oldSubject, newSubject, version)
		}
		val expectedSubjects = TestData.updatedSubjects()
		updatedSubjects should have length expectedSubjects.length
		updatedSubjects.zip(expectedSubjects).foreach { case (subject, expectedSubject) =>
			subject.id shouldEqual expectedSubject.id
			subject.name shouldEqual expectedSubject.name
			subject.aliases shouldEqual expectedSubject.aliases
			subject.category shouldEqual expectedSubject.category
			subject.properties shouldEqual expectedSubject.properties
			subject.relations shouldEqual expectedSubject.relations
		}
	}

	"Old Subjects" should "be updated correctly and extended with new Subjects" in {
		val subjects = sc.parallelize(TestData.oldSubjects())
		val newSubjects = sc.parallelize(TestData.newSubjects())
		val input = List(newSubjects, subjects).toAnyRDD()
		val updatedSubjects = DatasourceUpdate.run(input, sc).fromAnyRDD[Subject]().head.collect.toList.sortBy(_.id)
		val expectedSubjects = TestData.updatedAndNewSubjects().sortBy(_.id)
		updatedSubjects should have length expectedSubjects.length
		updatedSubjects.filter(_.isMaster) should have length 1
		updatedSubjects.filter(_.isSlave).zip(expectedSubjects.filter(_.isSlave)).foreach {
			case (subject, expectedSubject) =>
				subject.id shouldEqual expectedSubject.id
				subject.name shouldEqual expectedSubject.name
				subject.aliases shouldEqual expectedSubject.aliases
				subject.category shouldEqual expectedSubject.category
				subject.properties shouldEqual expectedSubject.properties
				subject.masterRelations shouldEqual expectedSubject.masterRelations
		}
	}
}
