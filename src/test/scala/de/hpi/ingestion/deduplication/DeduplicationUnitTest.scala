package de.hpi.ingestion.deduplication

import java.util.UUID

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.deduplication.similarity._
import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, Matchers}

class DeduplicationUnitTest
	extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {

	"compare" should "calculate a score regarding the configuration" in {
		val testSubjects = getSampleSubjects()
		val subject1 = testSubjects(0)
		val subject2 = testSubjects(1)

		val deduplication = defaultDeduplication
		deduplication.parseConfig()
		val score = deduplication.compare(subject1, subject2)
		val simScores = List (
			MongeElkan.compare(subject1.name.get, subject2.name.get) * 0.8,
			JaroWinkler.compare(subject1.name.get, subject2.name.get) * 0.7,
			ExactMatchString.compare(subject1.name.get, subject2.name.get) * 0.2
		)
		val expectedScore = simScores.sum / 3
		score shouldEqual expectedScore
	}

	it should "just return the weighted score if the configuration contains only one element" in {
		val testSubjects = getSampleSubjects()
		val subject1 = testSubjects(0)
		val subject2 = testSubjects(1)

		val deduplication = defaultDeduplication
		deduplication.parseConfig()
		val score = deduplication.compare(subject1, subject2)
		val expectedScore = MongeElkan.compare(subject1.name.get, subject2.name.get) * 0.8
		score shouldEqual expectedScore
	}

	"parseConfig" should "generate a configuration from a given path" in {
		val deduplication = defaultDeduplication
		deduplication.parseConfig()
		val expected = List(
			scoreConfig[String, SimilarityMeasure[String]]("name", MongeElkan, 0.8),
			scoreConfig[String, SimilarityMeasure[String]]("name", JaroWinkler, 0.7),
			scoreConfig[String, SimilarityMeasure[String]]("name", ExactMatchString, 0.2)
		)
		deduplication.config shouldEqual expected
	}

	"blocking" should "partition subjects regarding the value of the given key" in {
		val deduplication = defaultDeduplication
		val subjects = subjectRDD()
		val blockingScheme = new ListBlockingScheme()
		blockingScheme.setAttributes("city")
		val blocks = deduplication.blocking(subjects, blockingScheme)
		val expected = sc.parallelize(testBlocks)
		assertRDDEquals(expected, blocks)
	}

	"findDuplicates" should "return a list of tuple of duplicates" in {
		val testSubjects = getSampleSubjects()
		val subject1 = testSubjects(0)
		val subject4 = testSubjects(3)

		val deduplication = new Deduplication(0.35, "TestDeduplication", List("testSource"))
		deduplication.parseConfig()
		val duplicates = testBlocks()
			.map(_._2)
			.flatMap(deduplication.findDuplicates)
		val expected = List((subject1, subject4))
		duplicates shouldEqual expected
	}

	"buildDuplicatesClique" should "should add isDuplicate relation with a confidence for a list of tuples" in {
		val testSubjects = getSampleSubjects()
		val subject1 = testSubjects(0)
		val subject2 = testSubjects(1)
		val subject3 = testSubjects(2)
		val subject4 = testSubjects(3)

		val deduplication = defaultDeduplication
		val duplicates = List(
			(subject1, subject3, 0.8),
			(subject2, subject4, 0.8))
		val expectedRelation = Map("type" -> "isDuplicate", "confidence" -> "0.8")
		val expectedRelationNode1 = Map(subject3.id -> expectedRelation)
		val expectedRelationNode2 = Map(subject4.id -> expectedRelation)
		val version = deduplication.makeTemplateVersion()
		
		deduplication.buildDuplicatesClique(duplicates, version)

		subject1.relations shouldEqual expectedRelationNode1
		subject2.relations shouldEqual expectedRelationNode2

	}

	"evaluateBlocks" should "evaluate each block sorted by its size" in {
		val deduplication = defaultDeduplication
		val blocks = sc.parallelize(testBlocks)
		val evaluation = deduplication.evaluateBlocks(blocks, "Test comment")
		val expected = evaluationTestData
		expected.data shouldEqual evaluation.data
		expected.comment shouldEqual evaluation.comment
	}

	"addSymRelation" should "add a symmetric relation between two given nodes" in {
		val testSubjects = getSampleSubjects()
		val subject1 = testSubjects(0)
		val subject2 = testSubjects(1)
		val subject3 = testSubjects(2)
		val subject4 = testSubjects(3)

		val deduplication = defaultDeduplication()
		val sampleRelation = Map("type" -> "isDuplicate", "confidence" -> "0.8")
		val sampleVersion = deduplication.makeTemplateVersion()

		val expectedRelationNode1 = Map(subject2.id -> sampleRelation)
		val expectedRelationNode2 = Map(subject1.id -> sampleRelation)

		deduplication.addSymRelation(subject1, subject2, sampleRelation, sampleVersion)

		subject1.relations shouldEqual expectedRelationNode1
		subject2.relations shouldEqual expectedRelationNode2
	}

	def getSampleSubjects() : List[Subject] = {

		List( 
			Subject(
			name = Some("Volkswagen"),
			properties = Map("city" -> List("Berlin"))),
			Subject(
			name = Some("Audi GmbH"),
			properties = Map("city" -> List("Berlin"))),
			Subject(
			name = Some("Audy GmbH"),
			properties = Map("city" -> List("New York"))),
			Subject(
			name = Some("Volkswagen AG"),
			properties = Map("city" -> List("Berlin"))),
			Subject(
			name = Some("Porsche")),
			Subject(
			name = Some("Ferrari"))
		)
	}


	def subjectRDD(): RDD[Subject] = {
		val testSubjects = getSampleSubjects()
		val subject1 = testSubjects(0)
		val subject2 = testSubjects(1)
		val subject3 = testSubjects(2)
		val subject4 = testSubjects(3)
		val subject5 = testSubjects(4)
		val subject6 = testSubjects(5)

		sc.parallelize(List(subject1, subject2, subject3, subject4, subject5, subject6))
	}

	def testBlocks(): List[(List[String], List[Subject])] = {
		val testSubjects = getSampleSubjects()
		val subject1 = testSubjects(0)
		val subject2 = testSubjects(1)
		val subject3 = testSubjects(2)
		val subject4 = testSubjects(3)
		val subject5 = testSubjects(4)
		val subject6 = testSubjects(5)

		List((List("Berlin"), List(subject1, subject2, subject4)),
			(List("New York"), List(subject3)),
			(List("undefined"), List(subject5, subject6)))
	}

	def defaultDeduplication(): Deduplication = {
		new Deduplication(0.5, "TestDeduplication", List("testSource"))
	}

	def evaluationTestData(): BlockEvaluation = {
		BlockEvaluation(
			data = Map("Berlin" -> 3, "undefined" -> 2, "New York" -> 1),
			comment = Option("Test comment"))
	}
}
