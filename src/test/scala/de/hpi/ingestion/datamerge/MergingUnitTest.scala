package de.hpi.ingestion.datamerge

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, Matchers}

class MergingUnitTest extends FlatSpec with Matchers with SharedSparkContext with RDDComparisons {

	"addToMasterNode" should "create a new master node for a staging subject" in {
		val List(staging, master) = Merging.addToMasterNode(TestData.staging.head, TestData.version)

		staging.master shouldEqual master.id
		staging.master shouldEqual master.master
	}

	it should "add an existing master node for a staging subject" in {
		val version = TestData.version
		val score = 0.8
		val List(subject, staging) = Merging
			.addToMasterNode(TestData.subjects.head, TestData.staging.head, score, version)

		staging.master shouldEqual subject.master
		staging.relations(subject.id)("duplicate") shouldEqual score.toString
		subject.relations(staging.id)("duplicate") shouldEqual score.toString
	}

	"mergeProperties" should "merge all properties regarding the datasource" in {
		val subjects = TestData.mergedSubjects
		val result = Merging.mergeProperties(List(subjects(3), subjects(4), subjects(7)))
		val expected = TestData.subjects(2).properties

		result shouldEqual expected
	}

	"mergedRelations" should "merge all relations regarding the datatsource" in {
		val subjects = TestData.mergedSubjects
		val result = Merging.mergeRelations(List(subjects(3), subjects(4), subjects(7)))
		val expected = TestData.subjects(2).relations

		result shouldEqual expected
	}

	"masterRelations" should "return a Map with a master Relation to every slave node" in {
		val subjects = TestData.mergedSubjects
		val slaves = List(subjects(1), subjects(5), subjects(6))
		val result = Merging.masterRelations(slaves)
		val expected = subjects.head.relations.take(3)

		result shouldEqual expected
	}

	/*
	"getPrioritized" should "get an attribute sorted by datasources" in {
		val attributes = List("geo_postal", "id_implisense", "gen_urls")
		val subjects = TestData.variousDatasourceSubject
		val expected = List(
			List("10777", "10888"),
			List("1", "2"),
			List("http://curation.de", "http://google.de", "http://google.com")
		)

		(attributes, expected).zipped.foreach { case (attribute, expected) =>
			Merging.getPrioritized(attribute, subjects) shouldEqual expected
		}
	}

	"mergeAttributes" should "merge name, aliases, category and properties" in {
		val version = TestData.version
		val master = Subject.master(TestData.masterId)
		val masterManager = new SubjectManager(master, version)
		val slaves = TestData.subjects
		val mergedAttributes = Merging.mergeAttributes(masterManager, slaves)

	}
	*/

	"run" should "merge the staging subjects into the subjects regarding the duplicates" in {
		val staging = sc.parallelize(TestData.staging)
		val subjects = sc.parallelize(TestData.subjects)
		val duplicates = sc.parallelize(TestData.duplicates)
		val input = List(subjects, staging).toAnyRDD() ::: List(duplicates).toAnyRDD()
		val output = Merging.run(input, sc).head.asInstanceOf[RDD[Subject]]
		val expected = sc.parallelize(TestData.mergedSubjects)

		assertRDDEquals(output, expected)
	}

	/*
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
	*/
}
