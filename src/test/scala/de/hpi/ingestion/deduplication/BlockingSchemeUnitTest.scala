package de.hpi.ingestion.deduplication

import de.hpi.ingestion.datalake.models.Subject
import org.scalatest.{FlatSpec, Matchers}

class BlockingSchemeUnitTest extends FlatSpec with Matchers {
	val deduplication = new Deduplication(0.5, "TestDeduplication", List("testSource"))

	"DefaultBlockingScheme" should "generate proper keys" in {
		val blockingScheme = new DefaultBlockingScheme()
		val keys = testSubjects().map(blockingScheme.generateKey)
		keys.toSet shouldEqual defaultTestKeys
	}

	it should "generate an undefined block for subjects without names" in {
		val blockingScheme = new DefaultBlockingScheme()
		val keys = testUndefinedSubjects().map(blockingScheme.generateKey)
		keys.toSet shouldEqual defaultUndefinedTestKeys
	}

	"ListBlockingScheme" should "generate proper keys" in {
		val blockingScheme = new ListBlockingScheme()
		blockingScheme.setAttributes("name", "aliases", "category", "city")
		val keys = testSubjects().map(blockingScheme.generateKey)
		keys.toSet shouldEqual listTestKeys
	}

	it should "generate an undefined block for subjects with empty attributes" in {
		val blockingScheme = new ListBlockingScheme()
		blockingScheme.setAttributes("city")
		val keys = testUndefinedSubjects().map(blockingScheme.generateKey)
		keys.toSet shouldEqual listUndefinedTestKeys
	}


	def testSubjects(): List[Subject] = {
		List(Subject(
			name = Option("Audi AG"),
			category = Option("Cars"),
			properties = Map("city" -> List("Berlin", "New York"))),
			Subject(
				name = Option("Bayerische Motoren Werke AG"),
				aliases = List("bwm", "BMW"),
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

	def defaultTestKeys(): Set[List[String]] = {
		Set(List("({AG|null},LEGAL_FORM)"),
			List("({AG|null},LEGAL_FORM)",
				"({Bayerische|null},SECTOR)",
				"({Motoren|null},SECTOR)",
				"({Werke|null},SECTOR)"),
			List("({Lamborghini|null},PERSON_LAST_NAME)"),
			List("({Opel|null},PERSON_LAST_NAME)"),
			List("({Porsche|null},PERSON_LAST_NAME)"),
			List("undefined"))
	}

	def defaultUndefinedTestKeys(): Set[List[String]] = {
		Set(List("undefined"),
			List("undefined"),
			List("({Lamborghini|null},PERSON_LAST_NAME)"),
			List("({Opel|null},PERSON_LAST_NAME)"),
			List("undefined"),
			List("undefined"))
	}

	def listTestKeys(): Set[List[String]] = {
		Set(List("Audi AG", "Cars", "Berlin", "New York"),
			List("Bayerische Motoren Werke AG", "bwm", "BMW", "Cars"),
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
