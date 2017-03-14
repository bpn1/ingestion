package DataLake

import org.scalatest.{FlatSpec, Matchers}

class BlockingSchemeUnitTest extends FlatSpec with Matchers {
	"SimpleBlockingScheme" should "generate proper keys" in {
		val keys = SimpleBlockingScheme.generateKey()
		val expected = testKeys
		assert(expected == keys)
	}

	def testSubjects(): List[Subject] = {
		List(
			Subject(id = null, name = Option("Audi")),
			Subject(id = null, name = Option("BMW")),
			Subject(id = null, name = Option("Lamborghini")),
			Subject(id = null, name = Option("Opel")),
			Subject(id = null, name = Option("Porsche")),
			Subject(id = null, name = Option("VW"))
		)
	}
	def testKeys(): List[String] = {
		List("Aud", "BMW", "Lam", "Ope", "Por", "VW")
	}
}
