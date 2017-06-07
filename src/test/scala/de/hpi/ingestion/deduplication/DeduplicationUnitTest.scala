package de.hpi.ingestion.deduplication

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.deduplication.models.{Block, DuplicateCandidates}
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class DeduplicationUnitTest extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {
	"Compare" should "calculate a score regarding the configuration" in {
		val subject :: staging :: tail = TestData.testSubjects
		val simMeasures = TestData.testConfig()
		val score = Deduplication.compare(subject, staging, simMeasures)
		val expected = TestData.testConfigScore(subject, staging)
		score shouldEqual expected
	}

	it should "calculate the correct score for a complex configuration" in {
		val subject :: staging :: tail = TestData.testSubjects
		val simMeasures = TestData.complexTestConfig
		val score = Deduplication.compare(subject, staging, simMeasures)
		val expected = TestData.complexTestConfigScore(subject, staging)
		score shouldEqual expected
	}

	it should "just return the score if the configuration contains only one element" in {
		val subject :: staging :: tail = TestData.testSubjects
		val simMeasures = TestData.simpleTestConfig
		val score = Deduplication.compare(subject, staging, simMeasures)
		val expected = TestData.simpleTestConfigScore(subject, staging)
		score shouldEqual expected
	}

	"Config parsing" should "generate a configuration from a given path" in {
		val originalSettings = Deduplication.settings(false)
		val originalConfig = Deduplication.scoreConfigSettings(false)
		val originalName = Deduplication.configFile

		Deduplication.configFile = "test.xml"
		val expected = TestData.testConfig()
		Deduplication.parseConfig()
		Deduplication.scoreConfigSettings shouldEqual expected
		Deduplication.settings should not be empty

		Deduplication.scoreConfigSettings = originalConfig
		Deduplication.settings = originalSettings
		Deduplication.configFile = originalName
	}

	it should "be read before run is executed" in {
		val originalSettings = Deduplication.settings(false)
		val originalConfig = Deduplication.scoreConfigSettings(false)
		val originalName = Deduplication.configFile

		Deduplication.configFile = "test.xml"
		val expected = TestData.testConfig()
		Deduplication.assertConditions(Array[String]())
		Deduplication.scoreConfigSettings shouldEqual expected
		Deduplication.settings should not be empty

		Deduplication.scoreConfigSettings = originalConfig
		Deduplication.settings = originalSettings
		Deduplication.configFile = originalName
	}

	"Duplicates" should "be found, filtered and returned with their score" in {
		val originalSettings = Deduplication.settings(false)
		val originalConfig = Deduplication.scoreConfigSettings(false)

		Deduplication.scoreConfigSettings = TestData.testConfig()
		Deduplication.settings = Map("confidence" -> "0.35")
		val subjects = TestData.testSubjects
		val duplicates = Deduplication.findDuplicates(TestData.subjectBlocks(sc), sc).collect.toSet
		val expected = TestData.testDuplicates(subjects).toSet
		duplicates shouldEqual expected

		Deduplication.scoreConfigSettings = originalConfig
		Deduplication.settings = originalSettings
	}

	they should "be grouped into Duplicate Candidates" in {
		val originalSettings = Deduplication.settings(false)
		val subjects = TestData.testSubjects
		val duplicates = sc.parallelize(TestData.testDuplicates(subjects))
		Deduplication.settings = Map("stagingTable" -> "subject_wikidata")
		val duplicateCandidates = Deduplication.createDuplicateCandidates(duplicates)
		val expectedCandidates = sc.parallelize(TestData.duplicateCandidates(subjects))
		assertRDDEquals(duplicateCandidates, expectedCandidates)

		Deduplication.settings = originalSettings
	}

	they should "not be compared twice" in {
		val originalSettings = Deduplication.settings
		val originalConfig = Deduplication.scoreConfigSettings

		Deduplication.scoreConfigSettings = TestData.simpleTestConfig
		Deduplication.settings = Map("confidence" -> "0.0")
		val subjects = TestData.testSubjects
		val blocks = List(
			Block(key = "Vol", subjects = subjects.take(1), staging = List(subjects(1))),
			Block(key = "Auto", subjects = List(subjects.head, subjects(2)), staging = List(subjects(1), subjects(3))),
			Block(key = "Aud", subjects = List(subjects(2)), staging = List(subjects(3))))
		val duplicateCandidates = Deduplication.findDuplicates(sc.parallelize(blocks), sc).map(_.copy(_3 = 0.0))
		val expectedCandidates = TestData.distinctDuplicateCandidates(subjects, sc)
		assertRDDEquals(duplicateCandidates, expectedCandidates)

		Deduplication.scoreConfigSettings = originalConfig
		Deduplication.settings = originalSettings
	}

	"Deduplication" should "find duplicates and create duplicate candidates" in {
		val originalSettings = Deduplication.settings(false)
		val originalConfig = Deduplication.scoreConfigSettings(false)

		Deduplication.scoreConfigSettings = TestData.testConfig()
		Deduplication.settings = Map("stagingTable" -> "subject_wikidata", "confidence" -> "0.35")
		val subjects = TestData.subjects
		val stagings = TestData.stagings
		val input = List(sc.parallelize(subjects), sc.parallelize(stagings)).toAnyRDD()
		val duplicateCandidates = Deduplication.run(input, sc).fromAnyRDD[DuplicateCandidates]().head
		val expectedCandidates = sc.parallelize(TestData.trueDuplicateCandidates(subjects, stagings))
		assertRDDEquals(duplicateCandidates, expectedCandidates)

		Deduplication.scoreConfigSettings = originalConfig
		Deduplication.settings = originalSettings
	}
}
