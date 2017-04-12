package de.hpi.ingestion.dataimport.dbpedia

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.datalake.models.Version
import org.scalatest.{FlatSpec, Matchers}

class DBPediaMergeUnitTest extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {
	"mergeSubjects" should "merge a subject with his duplicates" in {
		val subject = TestData.subjectList().head
		val duplicate = TestData.dbpediaSubjectList().head
		val merged = DBPediaMerge.mergeSubjects(subject, List(duplicate), version())
		val expected = TestData.mergedSubject
		expected.properties shouldEqual merged.properties
	}

	it should "keep all first level attributes of the old subject" in {
		val subject = TestData.subjectList().head
		val duplicate = TestData.dbpediaSubjectList().head
		val merged = DBPediaMerge.mergeSubjects(subject, List(duplicate), version())
		val expected = TestData.mergedSubject
		expected.name shouldEqual merged.name
	}

	"joinSubjectsWithDuplicates" should "join each subject with each duplicate" in {
		val subjects = TestData.subjectList()
		val subjectRDD = sc.parallelize(subjects)
		val dbpedia = TestData.dbpediaSubjectList()
		val duplicates = TestData.candidatesJoinedRDD(sc, dbpedia, subjects)
		val joined = DBPediaMerge.joinSubjectsWithDuplicates(subjectRDD, duplicates)
		val expected = TestData.subjectDuplicatesRDD(sc, dbpedia, subjects)
		assertRDDEquals(expected, joined)
	}

	def version(): Version = Version("DBPediaMergeUnitTest", Nil, sc)
}
