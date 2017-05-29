package de.hpi.ingestion.datalake

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class SubjectManagerTest extends FlatSpec with Matchers with SharedSparkContext {

	"buildDuplicatesSCC" should "should add isDuplicate relation with a confidence for a list of tuples" in {
		val subjects = TestData.subjects
		val duplicates = List((subjects.head, subjects(2), 0.8), (subjects(1), subjects(3), 0.8))
		val expectedRelation = Map("type" -> "isDuplicate", "confidence" -> "0.8")
		val expectedRelationNode1 = Map(subjects(2).id -> expectedRelation)
		val expectedRelationNode2 = Map(subjects(3).id -> expectedRelation)
		val version = TestData.version(sc)
		SubjectManager.buildDuplicatesSCC(duplicates, version)

		subjects.head.relations shouldEqual expectedRelationNode1
		subjects(1).relations shouldEqual expectedRelationNode2
	}

	"addSymRelation" should "add a symmetric relation between two given nodes" in {
		val subjects = TestData.subjects
		val relation = Map("type" -> "isDuplicate", "confidence" -> "0.8")
		val version = TestData.version(sc)
		val expectedRelations = List(
			Map(subjects(1).id -> relation),
			Map(subjects.head.id -> relation))
		SubjectManager.addSymRelation(subjects.head, subjects(1), relation, version)
		subjects.zip(expectedRelations).foreach { case (subject, expectedRelation) =>
			subject.relations shouldEqual expectedRelation
		}
	}
}
