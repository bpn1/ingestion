package de.hpi.ingestion.deduplication

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.similarity._
import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, Matchers}

class DeduplicationUnitTest
	extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {

	"compare" should "calculate a score regarding the configuration" in {
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
		val deduplication = new Deduplication(0.35, "TestDeduplication", List("testSource"))
		deduplication.parseConfig()
		val duplicates = testBlocks()
			.map(_._2)
			.flatMap(deduplication.findDuplicates)
		val expected = List((subject1, subject4))
		duplicates shouldEqual expected
	}

	"mergingDuplicates" should "merge the properties of both subjects together" in {
		val deduplication = defaultDeduplication
		val duplicates = sc.parallelize(List(
			(subject1, subject4),
			(subject2, subject3)))
		val version = Version("DeduplicationUnitTest", List[String](), sc)
		val mergedSubject = deduplication.merging(duplicates, version)
		val expected = sc.parallelize(List(
			Subject(
				id = subject1.id,
				name = subject1.name,
				properties = subject1.properties ++ subject4.properties),
			Subject(
				id = subject2.id,
				name = subject2.name,
				properties = subject2.properties ++ subject3.properties)
		))
		assertRDDEquals(expected, mergedSubject)
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
		val deduplication = defaultDeduplication()
		val sampleRelation = Map("type" -> "isDuplicate", "confidence" -> "0.8")
		val sampleVersion = Version("DeduplicationUnitTest", List[String](), sc)

		val expectedRelationNode1 = Map(subject2.id -> sampleRelation)
		val expectedRelationNode2 = Map(subject1.id -> sampleRelation)

		deduplication.addSymRelation(subject1, subject2, sampleRelation, sampleVersion)

		subject1.relations shouldEqual expectedRelationNode1
		subject2.relations shouldEqual expectedRelationNode2
	}

	val subject1 = Subject(
		name = Some("Volkswagen"),
		properties = Map("city" -> List("Berlin"))
	)
	val subject2 = Subject(
		name = Some("Audi GmbH"),
		properties = Map("city" -> List("Berlin"))
	)
	val subject3 = Subject(
		name = Some("Audy GmbH"),
		properties = Map("city" -> List("New York"))
	)
	val subject4 = Subject(
		name = Some("Volkswagen AG"),
		properties = Map("city" -> List("Berlin"))
	)
	val subject5 = Subject(
		name = Some("Porsche")
	)
	val subject6 = Subject(
		name = Some("Ferrari")
	)

	def subjectRDD(): RDD[Subject] = {
		sc.parallelize(List(subject1, subject2, subject3, subject4, subject5, subject6))
	}

	def testBlocks(): List[(List[String], List[Subject])] = {
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
