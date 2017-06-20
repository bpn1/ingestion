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
			.flatMap { case (dupe, score) =>
				val duplicateCandidates = TestData.simpleDuplicateCandidates().headOption
				Merging.addToMasterNode(master, dupe, duplicateCandidates, version) }
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
		val duplicatecandidates = sc.parallelize(TestData.simpleDuplicateCandidates())
		val input = List(subjects).toAnyRDD() ++ List(staging).toAnyRDD() ++ List(duplicatecandidates).toAnyRDD()
		val mergedSubjects = Merging.run(input, sc).fromAnyRDD[Subject]().head.collect.toList
		val (List(mergedMaster), slaves) = mergedSubjects.partition(_.master.isEmpty)
		val expectedMaster = TestData.mergedMaster()
		val expectedSlaves = TestData.duplicates().sortBy(_.id)
		mergedMaster shouldEqual expectedMaster
		mergedMaster.name shouldEqual expectedMaster.name
		mergedMaster.category shouldEqual expectedMaster.category
		mergedMaster.aliases.toSet shouldEqual expectedMaster.aliases.toSet
		mergedMaster.properties shouldEqual expectedMaster.properties
		mergedMaster.relations shouldEqual expectedMaster.relations
		slaves.sortBy(_.id).zip(expectedSlaves).foreach { case (slave, expected) =>
			slave shouldEqual expected
			slave.name shouldEqual expected.name
			slave.category shouldEqual expected.category
			slave.aliases.toSet shouldEqual expected.aliases.toSet
			slave.properties shouldEqual expected.properties
			slave.relations shouldEqual expected.relations
		}
	}

	they should "be merged with the current subjects and create new subjects" in {
		val staging = sc.parallelize(TestData.smallerDuplicatesWithoutMaster())
		val subjects = sc.parallelize(TestData.existingSubjects())
		val duplicates = sc.parallelize(TestData.complexDuplicateCandidates())
		val input = List(subjects).toAnyRDD() ::: List(staging).toAnyRDD() ::: List(duplicates).toAnyRDD()
		val mergedSubjects = Merging.run(input, sc).fromAnyRDD[Subject]().head.collect.toList
		val (masters, slaves) = mergedSubjects.sortBy(_.id).partition(_.master.isEmpty)
		val newUUIDs = masters
			.filter(_.id != TestData.masterId)
			.sortBy(_.relations.keySet.head)
			.map(_.id)
		val expectedMasters = TestData.resultMasters(newUUIDs).sortBy(_.id)
		val expectedSlaves = TestData.resultSlaves(newUUIDs).sortBy(_.id)
		masters.zip(expectedMasters).foreach { case (master, expected) =>
			master shouldEqual expected
			master.name shouldEqual expected.name
			master.category shouldEqual expected.category
			master.aliases.toSet shouldEqual expected.aliases.toSet
			master.properties shouldEqual expected.properties
			master.relations shouldEqual expected.relations
		}
		slaves.zip(expectedSlaves).foreach { case (slave, expected) =>
			slave shouldEqual expected
			slave.name shouldEqual expected.name
			slave.category shouldEqual expected.category
			slave.aliases.toSet shouldEqual expected.aliases.toSet
			slave.properties shouldEqual expected.properties
			slave.relations shouldEqual expected.relations
		}
	}
}
