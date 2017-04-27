package de.hpi.ingestion.deduplication

import org.scalatest.{FlatSpec, Matchers}

class BlockingSchemeUnitTest extends FlatSpec with Matchers {
	"SimpleBlockingScheme" should "generate proper keys" in {
		val subjects = TestData.testSubjects
		val blockingScheme = new SimpleBlockingScheme
		val keys = subjects.map(blockingScheme.generateKey)
		val expected = TestData.simpleBlockingScheme
		keys.toSet shouldEqual expected.toSet
	}

	"ListBlockingScheme" should "generate proper keys" in {
		val subjects = TestData.testSubjects
		val blockingScheme = new ListBlockingScheme
		blockingScheme.setAttributes("geo_city", "gen_income")
		val keys = subjects.map(blockingScheme.generateKey)
		val expected = TestData.listblockingScheme
		keys.toSet shouldEqual expected.toSet
	}

	"MappedListBlockingScheme" should "generate proper keys" in {
		val subjects = TestData.testSubjects
		val blockingScheme = new MappedListBlockingScheme(
			attribute => attribute.substring(0, Math.min(3, attribute.length))
		)
		blockingScheme.setAttributes("name")
		val simpleBlockingScheme = new SimpleBlockingScheme

		subjects
			.map(subject => (blockingScheme.generateKey(subject), simpleBlockingScheme.generateKey(subject)))
			.foreach { case (keys, expected) =>
				keys shouldEqual expected
			}
	}

	it should "behave like ListBlockingScheme if no function is given" in {
		val subjects = TestData.testSubjects
		val attribute = "geo_city"
		val blockingScheme = new MappedListBlockingScheme
		blockingScheme.setAttributes(attribute)
		val listBlockingScheme = new ListBlockingScheme
		listBlockingScheme.setAttributes(attribute)

		subjects
			.map(subject => (blockingScheme.generateKey(subject), listBlockingScheme.generateKey(subject)))
			.foreach { case (keys, expected) =>
				keys shouldEqual expected
			}
	}
}
