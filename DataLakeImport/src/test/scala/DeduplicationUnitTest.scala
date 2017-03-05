package DataLake

import java.util.UUID

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec

object Deduplication extends Deduplication(
	confidence = 0.5,
	appName = "TestDeduplication",
	dataSources = List("testSource")
){
	def main(args: Array[String]): Unit = {
		if (args.length < 1) {
			println("Usage: config.xml_path merge(optionally)")
			return
		}
		val merge = if (args.length >= 2) args(1) == "merge"  else false
		this.config = parseConfig(args(0))

		deduplication(merge)
	}
}

class DeduplicationUnitTest extends FlatSpec with SharedSparkContext with RDDComparisons{
	"makePairs" should "generate a list containing all distinct pairs without regarding the order" in {
		val subjects = List(
			subject1,
			subject2,
			subject3
		)
		val pairs = Deduplication.makePairs(subjects)
		val expected = List(
			(subject1, subject2),
			(subject1, subject3),
			(subject2, subject3)
		)
		assert(expected === pairs)
	}

	"compare" should "calculate a score regarding the configuration" in {
		val config = List(
			scoreConfig("name", MongeElkan, 0.8),
			scoreConfig("name", JaroWinkler, 0.7),
			scoreConfig("name", ExactMatchString, 0.2)
		)
		val score = Deduplication.compare(subject1, subject2, config)
		val simScores = List (
			MongeElkan.compare(subject1.name.get, subject2.name.get) * 0.8,
			JaroWinkler.compare(subject1.name.get, subject2.name.get) * 0.7,
			ExactMatchString.compare(subject1.name.get, subject2.name.get) * 0.2
		)
		val expected = (
			subject1,
			subject2,
			simScores.sum / 3
		)
		assert(expected === score)
	}

	it should "just return the weighted score if the configuration contains only one element" in {
		val config = List(scoreConfig("name", MongeElkan, 0.8))
		val score = Deduplication.compare(subject1, subject2, config)
		var expected = (
			subject1,
			subject2,
			MongeElkan.compare(subject1.name.get, subject2.name.get) * 0.8
		)
		assert(expected === score)
	}

	"parseConfig" should "generate a configuration from a given path" in {
		val config = Deduplication.parseConfig(path)
		val expected = List(
			scoreConfig("name", MongeElkan, 0.8),
			scoreConfig("name", JaroWinkler, 0.7),
			scoreConfig("name", ExactMatchString, 0.2)
		)
		assert(expected === config)
	}

	"blockingByKey" should "partition subjects regarding the value of the given key" in {
		val subjects = subjectRDD()
		val blocks = Deduplication.blockingByKey(subjects, "city", sc)
		val expected = blockRDD()
		assertRDDEquals(expected, blocks)
	}

	"findDuplicates" should "return a list of tuple of duplicates" in {
		val duplicates = Deduplication.findDuplicates(blockRDD())
		val expected = List(
			(subject1, subject4),
			(subject2, subject3)
		)
	}

	"mergingDuplicates" should "should merge the properties of both subjects together" in {
		val duplicates = sc.parallelize(List(
			(subject1, subject4),
			(subject2, subject3)
		))
		val version = Deduplication.makeTemplateVersion()
		val mergedSubject = Deduplication.merging(duplicates, version)
		val expected = sc.parallelize(List(
			Subject(id = subject1.id, name = subject1.name, properties = subject1.properties ++ subject4.properties),
			Subject(id = subject2.id, name = subject2.name, properties = subject2.properties ++ subject3.properties)
		))
		assertRDDEquals(expected, mergedSubject)
	}

	val path = "./src/test/resources/config.xml"
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

	def blockRDD(): RDD[(String, Iterable[Subject])] = {
		sc.parallelize(List(
			("Berlin", List(subject1, subject2, subject4, subject5)),
			("Potsdam", List(subject1, subject5)),
			("New York", List(subject2, subject3, subject5))
		))
	}
}
