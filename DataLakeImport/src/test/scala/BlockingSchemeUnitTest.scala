package DataLake

import org.scalatest.{FlatSpec, Matchers}

class BlockingSchemeUnitTest extends FlatSpec with Matchers {
	val deduplication = new Deduplication(0.5, "TestDeduplication", List("testSource"))

	"SimpleBlockingScheme" should "generate proper keys" in {
		val blockingScheme = SimpleBlockingScheme
		val keys = testSubjects.map(blockingScheme.generateKey)
		keys.toSet shouldEqual simpleTestKeys.toSet
	}

	"ListBlockingScheme" should "generate proper keys" in {
		val blockingScheme = ListBlockingScheme
		blockingScheme.setAttributes("name", "aliases", "category", "properties")
		val keys = testSubjects.map(blockingScheme.generateKey)
		keys.toSet shouldEqual listTestKeys.toSet
	}

	def testSubjects(): List[Subject] = {
		List(
			Subject(
				id = null,
				name = Option("Audi"),
				category = Option("Cars"),
				properties = Map("city" -> List("Berlin", "New York"))),
			Subject(
				id = null,
				name = Option("BMW"),
				aliases = List("bwm", "Bayerische Motoren Werke"),
				category = Option("Cars")),
			Subject(
				id = null,
				name = Option("Lamborghini"),
				category = Option("Cars")),
			Subject(
				id = null,
				name = Option("Opel"),
				category = Option("Cars")),
			Subject(
				id = null,
				name = Option("Porsche"),
				category = Option("Cars")),
			Subject(
				id = null,
				name = Option("VW"),
				aliases = List("Volkswagen"),
				category = Option("Cars"),
				properties = Map("city" -> List("Berlin", "Potsdam")))
		)
	}
	def simpleTestKeys(): List[List[String]] = {
		List(
			List("Aud"),
			List("BMW"),
			List("Lam"),
			List("Ope"),
			List("Por"),
			List("VW"))
	}
	def listTestKeys(): List[List[String]] = {
		List(
			List("Audi", "Cars", "Berlin", "New York"),
			List("BMW", "bwm", "Bayerische Motoren Werke", "Cars"),
			List("Lamborghini", "Cars"),
			List("Opel", "Cars"),
			List("Porsche", "Cars"),
			List("VW", "Volkswagen", "Cars", "Berlin", "Potsdam"))
	}
}
