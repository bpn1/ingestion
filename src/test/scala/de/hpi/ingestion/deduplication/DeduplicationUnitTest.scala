package de.hpi.ingestion.deduplication

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.deduplication.models.{DuplicateCandidates, ScoreConfig}
import de.hpi.ingestion.deduplication.similarity._
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

import scala.collection.mutable

class DeduplicationUnitTest extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {
	"Compare" should "calculate a score regarding the configuration" in {
		val originalConfig = DeduplicationJob.config.toMap

		DeduplicationJob.config = TestData.testConfig()
		val subjects = TestData.testSubjects
		val score = DeduplicationJob.compare(subjects.head, subjects(1))
		val expected = TestData.testSubjectScore(subjects.head, subjects(1))
		score shouldEqual expected

		DeduplicationJob.config.clear()
		DeduplicationJob.config ++= originalConfig
	}

	it should "just return the weighted score if the configuration contains only one element" in {
		val originalConfig = DeduplicationJob.config.toMap

		val config = mutable.Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]]()
		config("name") = List(ScoreConfig(MongeElkan, 0.5))
		DeduplicationJob.config = config
		val subjects = TestData.testSubjects
		val score = DeduplicationJob.compare(subjects.head, subjects(1))
		val expected = MongeElkan.compare(subjects.head.name.get, subjects(1).name.get)
		score shouldEqual expected

		DeduplicationJob.config.clear()
		DeduplicationJob.config ++= originalConfig
	}

	"Config parsing" should "generate a configuration from a given path" in {
		val originalSettings = DeduplicationJob.settings.toMap
		val originalConfig = DeduplicationJob.config.toMap
		DeduplicationJob.settings shouldBe empty
		DeduplicationJob.config shouldBe empty

		val expected = TestData.parsedConfig
		DeduplicationJob.parseConfig()
		DeduplicationJob.config shouldEqual expected
		DeduplicationJob.settings should not be empty

		DeduplicationJob.config.clear()
		DeduplicationJob.config ++= originalConfig
		DeduplicationJob.settings.clear()
		DeduplicationJob.settings ++= originalSettings
	}

	it should "be read before run is executed" in {
		val originalSettings = DeduplicationJob.settings.toMap
		val originalConfig = DeduplicationJob.config.toMap

		val expected = TestData.parsedConfig
		DeduplicationJob.assertConditions(Array[String]())
		DeduplicationJob.config shouldEqual expected
		DeduplicationJob.settings should not be empty

		DeduplicationJob.config.clear()
		DeduplicationJob.config ++= originalConfig
		DeduplicationJob.settings.clear()
		DeduplicationJob.settings ++= originalSettings
	}

	"Duplicates" should "be found, filtered and returned with their score" in {
		val originalSettings = DeduplicationJob.settings.toMap
		val originalConfig = DeduplicationJob.config.toMap

		DeduplicationJob.config = TestData.parsedConfig
		DeduplicationJob.settings("confidence") = "0.35"
		val subjects = TestData.testSubjects
		val duplicates = DeduplicationJob.findDuplicates(TestData.subjectBlocks(subjects, sc)).collect.toSet
		val expected = TestData.testDuplicates(subjects).toSet
		duplicates shouldEqual expected

		DeduplicationJob.config.clear()
		DeduplicationJob.config ++= originalConfig
		DeduplicationJob.settings.clear()
		DeduplicationJob.settings ++= originalSettings
	}

	they should "be grouped into Duplicate Candidates" in {
		val originalSettings = DeduplicationJob.settings.toMap

		val subjects = TestData.testSubjects
		val duplicates = sc.parallelize(TestData.testDuplicates(subjects))
		DeduplicationJob.settings("stagingTable") = "subject_wikidata"
		val duplicateCandidates = DeduplicationJob.createDuplicateCandidates(duplicates)
		val expectedCandidates = sc.parallelize(TestData.duplicateCandidates(subjects))
		assertRDDEquals(duplicateCandidates, expectedCandidates)

		DeduplicationJob.settings.clear()
		DeduplicationJob.settings ++= originalSettings
	}

	"Deduplication" should "find duplicates and create duplicate candidates" in {
		val originalSettings = DeduplicationJob.settings.toMap
		val originalConfig = DeduplicationJob.config.toMap

		DeduplicationJob.config = TestData.parsedConfig
		DeduplicationJob.settings("confidence") = "0.35"
		DeduplicationJob.settings("stagingTable") = "subject_wikidata"
		val allSubjects = TestData.testSubjects
		val subjects = List(allSubjects.head, allSubjects(2), allSubjects(4), allSubjects(5))
		val staging = List(allSubjects(1), allSubjects(3))
		val input = List(sc.parallelize(subjects), sc.parallelize(staging)).toAnyRDD()
		val duplicateCandidates = DeduplicationJob.run(input, sc).fromAnyRDD[DuplicateCandidates]().head
		val expectedCandidates = sc.parallelize(TestData.trueDuplicateCandidates(allSubjects))
		assertRDDEquals(duplicateCandidates, expectedCandidates)

		DeduplicationJob.config.clear()
		DeduplicationJob.config ++= originalConfig
		DeduplicationJob.settings.clear()
		DeduplicationJob.settings ++= originalSettings
	}
}
