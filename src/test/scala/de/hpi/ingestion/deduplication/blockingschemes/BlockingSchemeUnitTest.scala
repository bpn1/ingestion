package de.hpi.ingestion.deduplication.blockingschemes

import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.deduplication.TestData
import org.scalatest.{FlatSpec, Matchers}

class BlockingSchemeUnitTest extends FlatSpec with Matchers {
	"SimpleBlockingScheme" should "generate proper keys" in {
		val subjects = TestData.testSubjects
		val blockingScheme = SimpleBlockingScheme("Test SimpleBS")
		val keys = subjects.map(blockingScheme.generateKey)
		val expected = TestData.simpleBlockingScheme
		keys.toSet shouldEqual expected.toSet
	}

	it should "generate a default undefined key if there is no name" in {
		val blockingScheme = SimpleBlockingScheme("Test SimpleBS")
		val subject = Subject()
		val key = blockingScheme.generateKey(subject)
		key shouldEqual List(blockingScheme.undefinedValue)
	}

	"ListBlockingScheme" should "generate proper keys" in {
		val subjects = TestData.testSubjects
		val blockingScheme = ListBlockingScheme("Test ListBS", "geo_city", "gen_income")
		val keys = subjects.map(blockingScheme.generateKey)
		val expected = TestData.listBlockingScheme
		keys.toSet shouldEqual expected.toSet
	}

	"MappedListBlockingScheme" should "generate proper keys" in {
		val subjects = TestData.testSubjects
		val function: String => String = attribute => attribute.substring(0, Math.min(3, attribute.length))
		val blockingScheme = MappedListBlockingScheme("Test MapBS", function, "name")
		val keys = subjects.map(blockingScheme.generateKey)
		val expected = TestData.mapBlockingScheme
		keys.toSet shouldEqual expected.toSet
	}

	it should "behave like ListBlockingScheme if no function is given" in {
		val subjects = TestData.testSubjects
		val attribute = "geo_city"
		val blockingScheme = MappedListBlockingScheme("Test MapBS", identity, attribute)
		val listBlockingScheme = ListBlockingScheme("Test ListBS", attribute)
		subjects
			.map(subject => (blockingScheme.generateKey(subject), listBlockingScheme.generateKey(subject)))
			.foreach { case (keys, expected) =>
				keys shouldEqual expected
			}
	}

	"GeoCoordsBlockingScheme" should "generate proper keys from coordinates" in {
		val subjects = TestData.testSubjects
		val blockingScheme = new GeoCoordsBlockingScheme
		val keys = subjects.map(blockingScheme.generateKey)
		val expected = TestData.geoCoordsBlockingScheme
		keys.toSet shouldEqual expected.toSet
	}

	it should "be created with the proper tag" in {
		val name = "Test Geo Scheme"
		val scheme = GeoCoordsBlockingScheme(name)
		scheme.tag shouldEqual name
		scheme.isInstanceOf[GeoCoordsBlockingScheme] shouldBe true
	}
}
