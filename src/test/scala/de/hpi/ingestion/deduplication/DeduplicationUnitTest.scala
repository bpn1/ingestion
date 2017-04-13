package de.hpi.ingestion.deduplication

import java.util.UUID

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.spark.{SparkConf, SparkContext}
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.similarity._
import org.scalatest.{FlatSpec, Matchers}

class DeduplicationUnitTest
	extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {

	"compare" should "calculate a score regarding the configuration" in {
		val testSubjects = getSampleSubjects()

		val deduplication = defaultDeduplication
		deduplication.parseConfig()
		val score = deduplication.compare(testSubjects(0), testSubjects(1))
		val simScores = List (
			MongeElkan.compare(testSubjects(0).name.get, testSubjects(1).name.get) * 0.8,
			JaroWinkler.compare(testSubjects(0).name.get, testSubjects(1).name.get) * 0.7,
			ExactMatchString.compare(testSubjects(0).name.get, testSubjects(1).name.get) * 0.2
		)
		val expectedScore = simScores.sum / 3
		score shouldEqual expectedScore
	}

	it should "just return the weighted score if the configuration contains only one element" in {
		val testSubjects = getSampleSubjects()

		val deduplication = defaultDeduplication
		deduplication.parseConfig()
		val score = deduplication.compare(testSubjects(0), testSubjects(1))
		val expectedScore = MongeElkan.compare(testSubjects(0).name.get, testSubjects(1).name.get) * 0.8
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
		val testSubjects = getSampleSubjects()
		val deduplication = defaultDeduplication
		val subjects = sc.parallelize(testSubjects)
		val blockingScheme = new ListBlockingScheme()
		blockingScheme.setAttributes("city")
		val blocks = deduplication.blocking(subjects, blockingScheme)
		val expected = sc.parallelize(testBlocks(testSubjects))
		assertRDDEquals(expected, blocks)
	}

	"findDuplicates" should "return a list of tuple of duplicates" in {
		val testSubjects = getSampleSubjects()

		val deduplication = new Deduplication(0.35, "TestDeduplication", List("testSource"))
		deduplication.parseConfig()
		val duplicates = testBlocks(testSubjects)
			.map(_._2)
			.flatMap(deduplication.findDuplicates)
		val expected = List((testSubjects(0), testSubjects(3)))
		duplicates shouldEqual expected
	}

	"buildDuplicatesClique" should "should add isDuplicate relation with a confidence for a list of tuples" in {
		val testSubjects = getSampleSubjects()

		val deduplication = defaultDeduplication
		val duplicates = List(
			(testSubjects(0), testSubjects(2), 0.8),
			(testSubjects(1), testSubjects(3), 0.8))
		val expectedRelation = Map("type" -> "isDuplicate", "confidence" -> "0.8")
		val expectedRelationNode1 = Map(testSubjects(2).id -> expectedRelation)
		val expectedRelationNode2 = Map(testSubjects(3).id -> expectedRelation)
		val version = getSampleVersion
		deduplication.buildDuplicatesSCC(duplicates, version)

		testSubjects(0).relations shouldEqual expectedRelationNode1
		testSubjects(1).relations shouldEqual expectedRelationNode2

	}

	"evaluateBlocks" should "evaluate each block sorted by its size" in {
		val testSubjects = getSampleSubjects()
		val deduplication = defaultDeduplication
		val blocks = sc.parallelize(testBlocks(testSubjects))
		val evaluation = deduplication.evaluateBlocks(blocks, "Test comment")
		val expected = evaluationTestData
		expected.data shouldEqual evaluation.data
		expected.comment shouldEqual evaluation.comment
	}

	"addSymRelation" should "add a symmetric relation between two given nodes" in {
		val testSubjects = getSampleSubjects()

		val deduplication = defaultDeduplication()
		val sampleRelation = Map("type" -> "isDuplicate", "confidence" -> "0.8")
		val sampleVersion = Version("DeduplicationUnitTest", List[String](), sc)

		val expectedRelationNode1 = Map(testSubjects(1).id -> sampleRelation)
		val expectedRelationNode2 = Map(testSubjects(0).id -> sampleRelation)

		deduplication.addSymRelation(testSubjects(0), testSubjects(1), sampleRelation, sampleVersion)

		 testSubjects(0).relations shouldEqual expectedRelationNode1
		 testSubjects(1).relations shouldEqual expectedRelationNode2
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

	def getSampleVersion : Version = {
		Version("SomeTestApp", Nil, sc)
	}

	def testBlocks(testSubjects: List[Subject]): List[(List[String], List[Subject])] = {

		List((List("Berlin"), List(testSubjects(0), testSubjects(1), testSubjects(3))),
			(List("New York"), List(testSubjects(2))),
			(List("undefined"), List(testSubjects(4), testSubjects(5))))
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
