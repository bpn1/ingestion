package de.hpi.ingestion.datamerge

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class MergingUnitTest extends FlatSpec with Matchers with SharedSparkContext {
	"addSymmetricRelation" should "add a symmetric relation between two given nodes" in {
		val subjects = TestData.testSubjects
		val relation = Map("type" -> "isDuplicate", "confidence" -> "0.8")
		val version = TestData.testVersion(sc)
		val expectedRelationNode1 = Map(subjects(1).id -> relation)
		val expectedRelationNode2 = Map(subjects.head.id -> relation)
		Merging.addSymmetricRelation(subjects.head, subjects(1), relation, version)

		subjects.head.relations shouldEqual expectedRelationNode1
		subjects(1).relations shouldEqual expectedRelationNode2
	}
}
