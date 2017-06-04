package de.hpi.ingestion.datamerge

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.scalatest.{FlatSpec, Matchers}

class MergingUnitTest extends FlatSpec with Matchers with SharedSparkContext {

	"Subject" should "be added to a master node" in {
		val duplicatesWithScore = TestData.simpleAttributeDuplicatesWithoutMaster()
		val master = TestData.master()
		val version = TestData.version("")
		val duplicatesWithRelation = duplicatesWithScore
			.flatMap { case (dupe, score) => Merging.addToMasterNode(master, dupe, score, version) }
			.filter(_ != master)
		val expectedDuplicates = TestData.simpleAttributeDuplicates()
		duplicatesWithRelation.zip(expectedDuplicates).foreach { case (dupe, expected) =>
			dupe shouldEqual expected
			dupe.name shouldEqual expected.name
			dupe.aliases shouldEqual expected.aliases
			dupe.category shouldEqual expected.category
			dupe.relations shouldEqual expected.relations
		}
	}

	"Slave properties" should "be merged" in {
		val duplicatesWithProperties = TestData.propertyDuplicates()
		val masterProperties = Merging.mergeProperties(duplicatesWithProperties)
		val expectedProperties = TestData.mergedProperties()
		masterProperties shouldEqual expectedProperties
	}

	"Slave relations" should "be merged" in {
		val master = TestData.master()
		val duplicatesWithRelations = TestData.relationDuplicates()
		val masterRelations = Merging.mergeRelations(master, duplicatesWithRelations)
		val expectedRelations = TestData.mergedRelations()
		masterRelations shouldEqual expectedRelations
	}

	"Simple slave attributes" should "be merged" in {
		val duplicates = TestData.simpleAttributeDuplicates()
		val master = TestData.master()
		val masterSM = TestData.masterSM(master)
		Merging.mergeSimpleAttributes(masterSM, duplicates)
		val expectedName = TestData.mergedName()
		val expectedAliases = TestData.mergedAliases()
		val expectedCategory = TestData.mergedCategory()
		master.name shouldEqual expectedName
		master.aliases shouldEqual expectedAliases
		master.category shouldEqual expectedCategory
	}

	"Master slave relations" should "be created" in {
		val duplicates = TestData.simpleAttributeDuplicates()
		val masterRelations = Merging.masterRelations(duplicates)
		val expectedRelations = TestData.masterRelations()
		masterRelations shouldEqual expectedRelations
	}

	"Duplicates" should "be merged into a master node" in {
		val master = TestData.master()
		val duplicates = TestData.duplicates()
		val version = TestData.version("Merging Test")
		val mergedMaster :: duplicateSubjects = Merging.mergeIntoMaster(master, duplicates, version)
		val expectedMaster = TestData.mergedMaster()
		mergedMaster shouldEqual expectedMaster
		mergedMaster.name shouldEqual expectedMaster.name
		mergedMaster.category shouldEqual expectedMaster.category
		mergedMaster.aliases shouldEqual expectedMaster.aliases
		mergedMaster.properties shouldEqual expectedMaster.properties
		mergedMaster.relations shouldEqual expectedMaster.relations
	}

	they should "be merged" in {
		val staging = sc.parallelize(TestData.duplicatesWithoutMaster())
		val subjects = sc.parallelize(List(TestData.master()))
		val duplicatecandidates = sc.parallelize(TestData.duplicateCandidates())
		val input = List(subjects).toAnyRDD() ++ List(staging).toAnyRDD() ++ List(duplicatecandidates).toAnyRDD()
		val mergedSubjects = Merging.run(input, sc).fromAnyRDD[Subject]().head.collect.toList
		val (List(mergedMaster), slaves) = mergedSubjects.partition(_.master.isEmpty)
		val expectedMaster = TestData.mergedMaster()
		mergedMaster shouldEqual expectedMaster
		mergedMaster.name shouldEqual expectedMaster.name
		mergedMaster.category shouldEqual expectedMaster.category
		mergedMaster.aliases.toSet shouldEqual expectedMaster.aliases.toSet
		mergedMaster.properties shouldEqual expectedMaster.properties
		mergedMaster.relations shouldEqual expectedMaster.relations
	}
}
