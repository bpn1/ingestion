package de.hpi.ingestion.datamerge

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class MergingUnitTest extends FlatSpec with Matchers with SharedSparkContext {
//	"addSymmetricRelation" should "add a symmetric relation between two given nodes" in {
//		val subjects = TestData.subjects.take(2)
//		val relation = Map("type" -> "isDuplicate", "confidence" -> "0.6")
//		val version = TestData.testVersion(sc)
//		Merging.addSymmetricRelation(subjects.head, subjects.last, relation, version)
//
//		subjects.head.relations(subjects.last.id) shouldEqual relation
//		subjects.last.relations(subjects.head.id) shouldEqual relation
//	}

//	"mergeInformation" should "merge slaves information into master node" in {
//		val master = TestData.masterNode
//		val duplicates = TestData.duplicates
//		val version = TestData.testVersion(sc)
//		Merging.mergeInformation(master, duplicates, version)
//		val expected = TestData.expectedProperties
//
//		master.properties shouldEqual expected
//	}
//
//	"appendDuplicates" should "append the new slaves to the master node" in {
//		val master = TestData.masterNode
//		val duplicates = TestData.duplicates
//		val relation = Map("type" -> "isDuplicate", "confidence" -> "0.6")
//		val version = TestData.testVersion(sc)
//		Merging.appendDuplicates(master, duplicates, version)
//		val expectedProperties = TestData.expectedProperties
//		val expectedRelations = TestData.expectedRelations
//
//		master.properties shouldEqual expectedProperties
//		master.relations shouldEqual expectedRelations
//		duplicates.candidates.foreach { case (duplicate, source, score) =>
//			duplicate.relations(master.id) shouldEqual relation
//		}
//	}
}
