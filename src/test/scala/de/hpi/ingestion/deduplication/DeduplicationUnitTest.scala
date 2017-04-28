package de.hpi.ingestion.deduplication

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.deduplication.models.DuplicateCandidates
import de.hpi.ingestion.deduplication.similarity._
import org.scalatest.{FlatSpec, Matchers}

class DeduplicationUnitTest extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {
	"compare" should "calculate a score regarding the configuration" in {
		val deduplication = TestData.defaultDeduplication
		deduplication.config = TestData.testConfig
		val subjects = TestData.testSubjects
		val score = deduplication.compare(subjects.head, subjects(1))
		val expected = TestData.testSubjectScore(subjects.head, subjects(1))

		score shouldEqual expected
	}

	it should "just return the weighted score if the configuration contains only one element" in {
		val deduplication = TestData.defaultDeduplication
		deduplication.config = TestData.testConfig.take(1)
		val subjects = TestData.testSubjects
		val score = deduplication.compare(subjects.head, subjects(1))
		val expected = MongeElkan.compare(subjects.head.name.get, subjects(1).name.get) * 0.8

		score shouldEqual expected
	}

	"parseConfig" should "generate a configuration from a given path" in {
		val deduplication = TestData.defaultDeduplication
		deduplication.parseConfig()
		val expected = TestData.testConfig

		deduplication.config shouldEqual expected
	}

	"blocking" should "partition subjects regarding the value of the given key" in {
		val deduplication = TestData.defaultDeduplication
		val subjects = TestData.testSubjects
		val subjectsRDD = sc.parallelize(subjects)
		val blockingSchemes = List(TestData.cityBlockingScheme, new SimpleBlockingScheme)
		val blocks = deduplication.blocking(subjectsRDD, blockingSchemes)
		val expectedBlocks = TestData.blocks(sc, subjects)

		(blocks, expectedBlocks).zipped.foreach { case (block, expectedBlock) =>
			assertRDDEquals(expectedBlock, block)
		}
	}

	"createDuplicateCandidates" should "build an RDD containing all candidates for deduplication for each subject" in {
		val testSubjects = TestData.testSubjects
		val deduplication = new Deduplication(0.35, "TestDeduplication", List("testSource"))
		deduplication.settings("stagingTable")= "subject_wikidata"
		val testSubject1 = testSubjects.head
		val testSubject2 = testSubjects(3)
		val testSubjectPair = List((testSubject1, testSubject2, 0.5))
		val testDuplicateCandidates = deduplication.createDuplicateCandidates(sc.parallelize(testSubjectPair))
		val expectedCandidate = DuplicateCandidates(testSubject1.id, List((testSubject2, "subject_wikidata", 0.5)))
		val expected = sc.parallelize(List(expectedCandidate))
		assertRDDEquals(testDuplicateCandidates, expected)
	}

	"findDuplicates" should "return a list of tuple of duplicates with their score" in {
		val deduplication = TestData.defaultDeduplication
		deduplication.config = TestData.testConfig
		val subjects = TestData.testSubjects
		val duplicates = deduplication.findDuplicates(subjects)
		val expected = TestData.testDuplicates(subjects)
		duplicates shouldEqual expected
	}

	"buildDuplicatesClique" should "should add isDuplicate relation with a confidence for a list of tuples" in {
		val deduplication = TestData.defaultDeduplication
		val subjects = TestData.testSubjects
		val duplicates = List((subjects.head, subjects(2), 0.8), (subjects(1), subjects(3), 0.8))
		val expectedRelation = Map("type" -> "isDuplicate", "confidence" -> "0.8")
		val expectedRelationNode1 = Map(subjects(2).id -> expectedRelation)
		val expectedRelationNode2 = Map(subjects(3).id -> expectedRelation)
		val version = TestData.testVersion(sc)
		deduplication.buildDuplicatesSCC(duplicates, version)

		subjects.head.relations shouldEqual expectedRelationNode1
		subjects(1).relations shouldEqual expectedRelationNode2

	}

	"evaluateBlocks" should "evaluate each block" in {
		val deduplication = TestData.defaultDeduplication
		val subjects = TestData.testSubjects
		val blocks = TestData.blocks(sc, subjects)
		val blockingSchemes = List(TestData.cityBlockingScheme, new SimpleBlockingScheme)
		val evaluationList = deduplication.evaluateBlocks(blocks, "Test comment", blockingSchemes)
		val evaluationData = evaluationList
			.map(blockingData => (blockingData._2, blockingData._3))
			.collect
			.toSet
		val expected = TestData.evaluationTestData.toSet
		evaluationData shouldEqual expected
	}

	"addSymRelation" should "add a symmetric relation between two given nodes" in {
		val deduplication = TestData.defaultDeduplication
		val subjects = TestData.testSubjects
		val relation = Map("type" -> "isDuplicate", "confidence" -> "0.8")
		val version = TestData.testVersion(sc)
		val expectedRelationNode1 = Map(subjects(1).id -> relation)
		val expectedRelationNode2 = Map(subjects.head.id -> relation)
		deduplication.addSymRelation(subjects.head, subjects(1), relation, version)

		subjects.head.relations shouldEqual expectedRelationNode1
		subjects(1).relations shouldEqual expectedRelationNode2
	}
}
