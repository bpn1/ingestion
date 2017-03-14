package DataLake

import java.util.UUID

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, Matchers}

class DeduplicationUnitTest
	extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {

	val deduplication = new Deduplication(0.5, "TestDeduplication", List("testSource"))

	"compare" should "calculate a score regarding the configuration" in {
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
		deduplication.parseConfig()
		val score = deduplication.compare(subject1, subject2)
		val expectedScore = MongeElkan.compare(subject1.name.get, subject2.name.get) * 0.8
		score shouldEqual expectedScore
	}

	"parseConfig" should "generate a configuration from a given path" in {
		deduplication.parseConfig()
		val expected = List(
			scoreConfig[String, SimilarityMeasure[String]]("name", MongeElkan, 0.8),
			scoreConfig[String, SimilarityMeasure[String]]("name", JaroWinkler, 0.7),
			scoreConfig[String, SimilarityMeasure[String]]("name", ExactMatchString, 0.2)
		)
		deduplication.config shouldEqual expected
	}

//	"blockingByKey" should "partition subjects regarding the value of the given key" in {
//		val subjects = subjectRDD()
//		val blocks = deduplication.blockingByKey(subjects, "city", sc)
//		val expected = blockRDD()
//		assertRDDEquals(expected, blocks)
//	}

//	"findDuplicates" should "return a list of tuple of duplicates" in {
//		TODO: make used rdds fields in deduplication
//		val duplicates = deduplication.findDuplicates(blockRDD())	// RDD or List as input?
//		val expected = List(
//			(subject1, subject4),
//			(subject2, subject3)
//		)
//	}

	"mergingDuplicates" should "should merge the properties of both subjects together" in {
		val duplicates = sc.parallelize(List(
			(subject1, subject4),
			(subject2, subject3)
		))
		val version = deduplication.makeTemplateVersion()
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

	val path = "config.xml"
	val subject1 = Subject(
		id = UUID.randomUUID(),
		name = Some("Volkswagen"),
		properties = Map("city" -> List("Berlin", "Potsdam"))
	)
	val subject2 = Subject(
		id = UUID.randomUUID(),
		name = Some("Audi GmbH"),
		properties = Map("city" -> List("Berlin", "New York"))
	)
	val subject3 = Subject(
		id = UUID.randomUUID(),
		name = Some("Audy GmbH"),
		properties = Map("city" -> List("New York"))
	)
	val subject4 = Subject(
		id = UUID.randomUUID(),
		name = Some("Volkswagen AG"),
		properties = Map("city" -> List("Berlin"))
	)
	val subject5 = Subject(
		id = UUID.randomUUID(),
		name = Some("Porsche")
	)

	def subjectRDD(): RDD[Subject] = {
		sc.parallelize(List(
			subject1,
			subject2,
			subject3,
			subject4,
			subject5
		))
	}

	def testBlocks(): List[(String, List[Subject])] = {
		List(
			("Berlin", List(subject1, subject2, subject4, subject5)),
			("Potsdam", List(subject1, subject5)),
			("New York", List(subject2, subject3, subject5)))
	}
}
