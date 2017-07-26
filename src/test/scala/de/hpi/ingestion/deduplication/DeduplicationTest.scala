package de.hpi.ingestion.deduplication

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.deduplication.models.Duplicates
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class DeduplicationTest extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {
	"Compare" should "calculate a score regarding the configuration" in {
		val subject = TestData.subjects.head
		val staging = TestData.stagings.head
		val config = TestData.testConfig()
		val score = Deduplication.compare(subject, staging, config)
		val expected = TestData.testConfigScore
		score shouldEqual expected
	}

	it should "calculate the correct score for a complex configuration" in {
		val subject = TestData.subjects.head
		val staging = TestData.stagings.head
		val config = TestData.complexTestConfig
		val score = Deduplication.compare(subject, staging, config)
		val expected = TestData.complexTestConfigScore
		score shouldEqual expected
	}

	it should "just return the score if the configuration contains only one element" in {
		val subject = TestData.subjects.head
		val staging = TestData.stagings.head
		val config = TestData.simpleTestConfig
		val score = Deduplication.compare(subject, staging, config)
		val expected = TestData.simpleTestConfigScore
		score shouldEqual expected
	}

	"Duplicates" should "be found, filtered and returned with their score" in {
		val originalSettings = Deduplication.settings(false)
		val originalConfig = Deduplication.scoreConfigSettings(false)

		Deduplication.scoreConfigSettings = TestData.testConfig()
		Deduplication.settings = Map("confidence" -> "0.9")
		val duplicates = Deduplication.findDuplicates(TestData.subjectBlocks(sc), sc)
		val expected = TestData.filteredDuplicates(sc)
		assertRDDEquals(duplicates, expected)

		Deduplication.scoreConfigSettings = originalConfig
		Deduplication.settings = originalSettings
	}

	they should "be grouped into Duplicate Candidates" in {
		val originalSettings = Deduplication.settings(false)

		Deduplication.settings = Map("stagingTable" -> "subject_wikidata")
		val subjects = TestData.subjects
		val stagings = TestData.stagings
		val duplicates = TestData.testDuplicates(sc)
		val candidates = Deduplication.createDuplicates(duplicates)
		val expectedCandidates = sc.parallelize(TestData.createdDuplicateCandidates(subjects, stagings))
		assertRDDEquals(candidates, expectedCandidates)

		Deduplication.settings = originalSettings
	}

	"Deduplication" should "find duplicates and create duplicate candidates" in {
		val originalSettings = Deduplication.settings(false)
		val originalConfig = Deduplication.scoreConfigSettings(false)

		Deduplication.scoreConfigSettings = TestData.testConfig()
		Deduplication.settings = Map("stagingTable" -> "subject_wikidata", "confidence" -> "0.9")

		val subjects = TestData.subjects
		val stagings = TestData.stagings
		val input = List(sc.parallelize(subjects), sc.parallelize(stagings)).toAnyRDD()
		val duplicates = Deduplication.run(input, sc).fromAnyRDD[Duplicates]().head
		val expected = sc.parallelize(TestData.trueDuplicates(subjects, stagings))
		assertRDDEquals(duplicates, expected)

		Deduplication.scoreConfigSettings = originalConfig
		Deduplication.settings = originalSettings
	}
}
