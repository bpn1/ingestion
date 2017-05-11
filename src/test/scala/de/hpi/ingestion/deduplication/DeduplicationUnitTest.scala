package de.hpi.ingestion.deduplication

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.deduplication.models.{DuplicateCandidates, ScoreConfig}
import de.hpi.ingestion.deduplication.similarity._
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class DeduplicationUnitTest extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {
	"Compare" should "calculate a score regarding the configuration" in {
		val originalConfig = Deduplication.config

		Deduplication.config = TestData.testConfig()
		val subjects = TestData.testSubjects
		val score = Deduplication.compare(subjects.head, subjects(1), Deduplication.config)
		val expected = TestData.testSubjectScore(subjects.head, subjects(1))
		score shouldEqual expected

		Deduplication.config = originalConfig
	}

	it should "just return the weighted score if the configuration contains only one element" in {
		val originalConfig = Deduplication.config

		Deduplication.config = Map("name" -> List(ScoreConfig[String, SimilarityMeasure[String]](MongeElkan, 0.5)))
		val subjects = TestData.testSubjects
		val score = Deduplication.compare(subjects.head, subjects(1), Deduplication.config)
		val expected = MongeElkan.compare(subjects.head.name.get, subjects(1).name.get)
		score shouldEqual expected

		Deduplication.config = originalConfig
	}

	"Config parsing" should "generate a configuration from a given path" in {
		val originalSettings = Deduplication.settings
		val originalConfig = Deduplication.config
		Deduplication.settings shouldBe empty
		Deduplication.config shouldBe empty

		val expected = TestData.parsedConfig
		Deduplication.parseConfig()
		Deduplication.config shouldEqual expected
		Deduplication.settings should not be empty

		Deduplication.config = originalConfig
		Deduplication.settings = originalSettings
	}

	it should "be read before run is executed" in {
		val originalSettings = Deduplication.settings
		val originalConfig = Deduplication.config

		val expected = TestData.parsedConfig
		Deduplication.assertConditions(Array[String]())
		Deduplication.config shouldEqual expected
		Deduplication.settings should not be empty

		Deduplication.config = originalConfig
		Deduplication.settings = originalSettings
	}

	"Duplicates" should "be found, filtered and returned with their score" in {
		val originalSettings = Deduplication.settings
		val originalConfig = Deduplication.config

		Deduplication.config = TestData.parsedConfig
		Deduplication.settings = Map("confidence" -> "0.35")
		val subjects = TestData.testSubjects
		val duplicates = Deduplication.findDuplicates(TestData.subjectBlocks(subjects, sc), sc).collect.toSet
		val expected = TestData.testDuplicates(subjects).toSet
		duplicates shouldEqual expected

		Deduplication.config = originalConfig
		Deduplication.settings = originalSettings
	}

	they should "be grouped into Duplicate Candidates" in {
		val originalSettings = Deduplication.settings

		val subjects = TestData.testSubjects
		val duplicates = sc.parallelize(TestData.testDuplicates(subjects))
		Deduplication.settings = Map("stagingTable" -> "subject_wikidata")
		val duplicateCandidates = Deduplication.createDuplicateCandidates(duplicates)
		val expectedCandidates = sc.parallelize(TestData.duplicateCandidates(subjects))
		assertRDDEquals(duplicateCandidates, expectedCandidates)

		Deduplication.settings = originalSettings
	}

	"Deduplication" should "find duplicates and create duplicate candidates" in {
		val originalSettings = Deduplication.settings
		val originalConfig = Deduplication.config

		Deduplication.config = TestData.parsedConfig
		Deduplication.settings = Map("stagingTable" -> "subject_wikidata", "confidence" -> "0.35")
		val allSubjects = TestData.testSubjects
		val subjects = List(allSubjects.head, allSubjects(2), allSubjects(4), allSubjects(5))
		val staging = List(allSubjects(1), allSubjects(3))
		val input = List(sc.parallelize(subjects), sc.parallelize(staging)).toAnyRDD()
		val duplicateCandidates = Deduplication.run(input, sc).fromAnyRDD[DuplicateCandidates]().head
		val expectedCandidates = sc.parallelize(TestData.trueDuplicateCandidates(allSubjects))
		assertRDDEquals(duplicateCandidates, expectedCandidates)

		Deduplication.config = originalConfig
		Deduplication.settings = originalSettings
	}
}
