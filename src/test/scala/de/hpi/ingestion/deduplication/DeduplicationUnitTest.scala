package de.hpi.ingestion.deduplication

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.deduplication.models.{DuplicateCandidates, ScoreConfig}
import de.hpi.ingestion.deduplication.similarity._
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

class DeduplicationUnitTest extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {
	"compare" should "calculate a score regarding the configuration" in {
		DeduplicationJob.config = TestData.testConfig()
		val subjects = TestData.testSubjects
		val score = DeduplicationJob.compare(subjects.head, subjects(1))
		val expected = TestData.testSubjectScore(subjects.head, subjects(1))

		score shouldEqual expected
	}

	it should "just return the weighted score if the configuration contains only one element" in {
		val config = mutable.Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]]()
		config("name") = List(ScoreConfig(MongeElkan, 0.5))
		DeduplicationJob.config = config
		val subjects = TestData.testSubjects
		val score = DeduplicationJob.compare(subjects.head, subjects(1))
		val expected = MongeElkan.compare(subjects.head.name.get, subjects(1).name.get)

		score shouldEqual expected
	}

	"parseConfig" should "generate a configuration from a given path" in {
		DeduplicationJob.parseConfig()
		val expected = TestData.parsedConfig
		DeduplicationJob.config shouldEqual expected
	}

	"createDuplicateCandidates" should "build an RDD containing all candidates for deduplication for each subject" in {
		val testSubjects = TestData.testSubjects
		DeduplicationJob.settings("stagingTable") = "subject_wikidata"
		val testSubject1 = testSubjects.head
		val testSubject2 = testSubjects(3)
		val testSubjectPair = List((testSubject1, testSubject2, 0.5))
		val testDuplicateCandidates = DeduplicationJob.createDuplicateCandidates(sc.parallelize(testSubjectPair))
		val expectedCandidate = DuplicateCandidates(testSubject1.id, List((testSubject2, "subject_wikidata", 0.5)))
		val expected = sc.parallelize(List(expectedCandidate))
		assertRDDEquals(testDuplicateCandidates, expected)
	}

	"findDuplicates" should "return a list of tuple of duplicates with their score" in {
		DeduplicationJob.config = TestData.parsedConfig
		DeduplicationJob.settings("confidence") = "0.35"
		val subjects = TestData.testSubjects
		val duplicates = DeduplicationJob.findDuplicates(TestData.subjectBlocks(subjects, sc)).collect.toSet
		val expected = TestData.testDuplicates(subjects).toSet
		duplicates shouldEqual expected
	}
}
