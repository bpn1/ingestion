package de.hpi.ingestion.deduplication

import java.util.UUID

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.deduplication.models._
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

	"RoughlyEqualNumbers" should "calculate the similarity of two numbers, but not as strings" in {
			val testSubjects = getSampleSubjects()
			val expectedScore1 = 0.98
			val expectedScore2 = 0.02

			val computedScore1 = RoughlyEqualNumbers.compare(
				testSubjects(0).properties("grossIncome").head,
				testSubjects(1).properties("grossIncome").head
			)

			val computedScore2 = RoughlyEqualNumbers.compare(
				testSubjects(2).properties("grossIncome").head,
				testSubjects(3).properties("grossIncome").head
			)

			computedScore1 shouldEqual expectedScore1
			computedScore2 shouldEqual computedScore2
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
			ScoreConfig[String, SimilarityMeasure[String]]("name", MongeElkan, 0.8),
			ScoreConfig[String, SimilarityMeasure[String]]("name", JaroWinkler, 0.7),
			ScoreConfig[String, SimilarityMeasure[String]]("name", ExactMatchString, 0.2)
		)
		deduplication.config shouldEqual expected
	}

	"blocking" should "partition subjects regarding the value of the given key" in {
		val testSubjects = getSampleSubjects()
		val deduplication = defaultDeduplication
		val subjects = sc.parallelize(testSubjects)
		val blockingSchemes = List(new ListBlockingScheme())
		blockingSchemes.foreach(_.setAttributes("city"))
		val blocks = deduplication.blocking(subjects, blockingSchemes)
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

	"turnDistanceIntoScore" should "compute the right score for given distances and scale factors" in {
		val testValues = List(0.0,1.0,5.1,21.0,312.0)

		val testScale1 = 1
		val testScale2 = 2

		val computedValues = List(
			EuclidianDistance.turnDistanceIntoScore(testValues(0), testScale1),
			EuclidianDistance.turnDistanceIntoScore(testValues(1), testScale1),
			EuclidianDistance.turnDistanceIntoScore(testValues(2), testScale1),
			EuclidianDistance.turnDistanceIntoScore(testValues(3), testScale1),
			EuclidianDistance.turnDistanceIntoScore(testValues(4), testScale1),
			EuclidianDistance.turnDistanceIntoScore(testValues(0), testScale2),
			EuclidianDistance.turnDistanceIntoScore(testValues(1), testScale2),
			EuclidianDistance.turnDistanceIntoScore(testValues(2), testScale2),
			EuclidianDistance.turnDistanceIntoScore(testValues(3), testScale2),
			EuclidianDistance.turnDistanceIntoScore(testValues(4), testScale2)
		)

		val expectedValues = List(1.0,1.0,0.75,0.5,0.0,1.0,1.0,1.0,0.75,0.0)

		(computedValues,expectedValues).zipped.foreach((x,y) => x shouldEqual y)

	}

	 "computedDistance" should "compute the distnace between two points in terms of kilometers" in {
		val testSubjects = getSampleSubjects()
		val expectedDistance = 111.70485139435159
		val computedDistance = EuclidianDistance.computeDistance(
			testSubjects(4).properties("geoCoords")(0).toDouble,
			testSubjects(4).properties("geoCoords")(1).toDouble,
			testSubjects(5).properties("geoCoords")(0).toDouble,
			testSubjects(5).properties("geoCoords")(1).toDouble
		)

		expectedDistance shouldEqual computedDistance
	}

	"EuclidianDistance" should "compute correct score for two given points" in {
		val testSubjects = getSampleSubjects()

		val testGeoPoint1 = testSubjects(1).properties("geoCoords").mkString(",")
		val testGeoPoint2 = testSubjects(2).properties("geoCoords").mkString(",")
		val testGeoPoint3 = testSubjects(0).properties("geoCoords").mkString(",")


		val computedScore1 = EuclidianDistance.compare(
			testGeoPoint1,
			testGeoPoint2
		)

		val computedScore2 = EuclidianDistance.compare(
			testGeoPoint1,
			testGeoPoint3
		)

		val computedScore3 = EuclidianDistance.compare(
			testGeoPoint1,
			testGeoPoint1
		)

		val expectedScore1 = 0.75
		val expectedScore2 = 0.5
		val expectedScore3 = 1.0

		computedScore1 shouldEqual expectedScore1
		computedScore2 shouldEqual expectedScore2
		computedScore3 shouldEqual expectedScore3

	}

	def getSampleSubjects() : List[Subject] = {

		List(
			Subject(
				name = Some("Volkswagen"),
				properties = Map(
					"city" -> List("Berlin"),
					"geoCoords" -> List("52","11"),
					"grossIncome" -> List("1000000")
				)
			),
			Subject(
				name = Some("Audi GmbH"),
				properties = Map(
					"city" -> List("Berlin"),
					"geoCoords" -> List("52","14"),
					"grossIncome" -> List("980000")
				)
			),
			Subject(
				name = Some("Audy GmbH"),
				properties = Map(
					"city" -> List("New York"),
					"geoCoords" -> List("52","13"),
					"grossIncome" -> List("600")
					)
			),
			Subject(
				name = Some("Volkswagen AG"),
				properties = Map(
					"city" -> List("Berlin"),
					"grossIncome" -> List("12")
					)
				),
			Subject(
				name = Some("Porsche"),
				properties = Map(
					"geoCoords" -> List("52","13")
					)
				),
			Subject(
				name = Some("Ferrari"),
				properties = Map(
					"geoCoords" -> List("53","14")
					)
				)
		)
	}

	def getSampleVersion : Version = {
		Version("SomeTestApp", Nil, sc)
	}

	def testBlocks(testSubjects: List[Subject]): List[(List[List[String]], List[Subject])] = {
		List((List(List("Berlin")),  List(testSubjects(0), testSubjects(1), testSubjects(3))),
			(List(List("New York")), List(testSubjects(2))),
			(List(List("undefined")), List(testSubjects(4), testSubjects(5))))
	}

	def defaultDeduplication(): Deduplication = {
		new Deduplication(0.5, "TestDeduplication", List("testSource"))
	}

	def evaluationTestData(): BlockEvaluation = {
		BlockEvaluation(
			data = Map(List("Berlin") -> 3, List("undefined") -> 2, List("New York") -> 1),
			comment = Option("Test comment"))
	}
}
