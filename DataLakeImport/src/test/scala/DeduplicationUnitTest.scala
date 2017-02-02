package DataLake

import java.util.UUID

import org.scalatest.FlatSpec

class DeduplicationUnitTest extends FlatSpec{
	val path = "./src/test/resources/config.xml"

	"parseConfig" should "update return a List of similarityMeasures corresponding to xml config file" in {
		val config = Deduplication.parseConfig(path)
		assert(config.length == 2)
		assert(config.head.equals(scoreConfig[String, ExactMatchString.type]("name", ExactMatchString, 1.0)))
		assert(config(1).equals(scoreConfig[String, MongeElkan.type]("name", MongeElkan, 0.8)))
	}

	"score" should "calculate similarity of two subjects" in {
		val config = Deduplication.parseConfig(path)
		val subject1 = Subject(UUID.randomUUID(), Option("henkan"))
		val subject2 = Subject(UUID.randomUUID, Option("henka"))
		val score = Deduplication.score(subject1, subject2, config)
		assert(score == 1.0 * ExactMatchString.score(subject1.name.get, subject2.name.get) + 0.8 * MongeElkan.score(subject1.name.get, subject2.name.get) / 2)
	}
}
