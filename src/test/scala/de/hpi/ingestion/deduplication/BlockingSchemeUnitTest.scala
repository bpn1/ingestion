package de.hpi.ingestion.deduplication

import de.hpi.ingestion.datalake.models.Subject
import org.scalatest.{FlatSpec, Matchers}

class BlockingSchemeUnitTest extends FlatSpec with Matchers {
	"SimpleBlockingScheme" should "generate proper keys" in {
		val blockingScheme = new SimpleBlockingScheme()
		val keys = testSubjects.map(blockingScheme.generateKey)
		keys.toSet shouldEqual simpleTestKeys
	}

	it should "generate an undefined block for subjects without names" in {
		val blockingScheme = new SimpleBlockingScheme()
		val keys = testUndefinedSubjects().map(blockingScheme.generateKey)
		keys.toSet shouldEqual simpleUndefinedTestKeys
	}

	"ListBlockingScheme" should "generate proper keys" in {
		val blockingScheme = new ListBlockingScheme()
		blockingScheme.setAttributes("name", "aliases", "category", "city")
		val keys = testSubjects.map(blockingScheme.generateKey)
		keys.toSet shouldEqual listTestKeys
	}

	it should "generate an undefined block for subjects with an empty key" in {
		val blockingScheme = new ListBlockingScheme()
		blockingScheme.setAttributes("city")
		val keys = testSubjects.map(blockingScheme.generateKey)
		keys.toSet shouldEqual listUndefinedTestKeys
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

	def testSubjects(): List[Subject] = {
		List(Subject(
			name = Option("Audi"),
			category = Option("Cars"),
			properties = Map("city" -> List("Berlin", "New York"))),
			Subject(
				name = Option("BMW"),
				aliases = List("bwm", "Bayerische Motoren Werke"),
				category = Option("Cars")),
			Subject(
				name = Option("Lamborghini"),
				category = Option("Cars")),
			Subject(
				name = Option("Opel"),
				category = Option("Cars")),
			Subject(
				name = Option("Porsche"),
				category = Option("Cars")),
			Subject(
				name = Option("VW"),
				aliases = List("Volkswagen"),
				category = Option("Cars"),
				properties = Map("city" -> List("Berlin", "Potsdam"))))
	}

	def testUndefinedSubjects(): List[Subject] = {
		List(Subject(
			name = None,
			category = Option("Cars"),
			properties = Map("city" -> List("Berlin", "New York"))),
			Subject(
				name = Option("BMW"),
				aliases = List("bwm", "Bayerische Motoren Werke"),
				category = Option("Cars")),
			Subject(
				name = Option("Lamborghini"),
				category = Option("Cars")),
			Subject(
				name = Option("Opel"),
				category = Option("Cars")),
			Subject(
				name = None,
				category = Option("Cars")),
			Subject(
				name = Option("VW"),
				aliases = List("Volkswagen"),
				category = Option("Cars"),
				properties = Map("city" -> List("Berlin", "Potsdam"))))
	}

	def simpleTestKeys(): Set[List[String]] = {
		Set(List("Aud"), List("BMW"), List("Lam"), List("Ope"), List("Por"), List("VW"))
	}

	def simpleUndefinedTestKeys(): Set[List[String]] = {
		Set(List("undefined"), List("BMW"), List("Lam"), List("Ope"), List("VW"))
	}

	def listTestKeys(): Set[List[String]] = {
		Set(List("Audi", "Cars", "Berlin", "New York"),
			List("BMW", "bwm", "Bayerische Motoren Werke", "Cars"),
			List("Lamborghini", "Cars"),
			List("Opel", "Cars"),
			List("Porsche", "Cars"),
			List("VW", "Volkswagen", "Cars", "Berlin", "Potsdam"))
	}

	def listUndefinedTestKeys(): Set[List[String]] = {
		Set(List("Berlin", "New York"),
			List("undefined"),
			List("Berlin", "Potsdam"))
	}
}
