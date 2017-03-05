import org.scalatest.{FlatSpec, Matchers}

class BagTest extends FlatSpec with Matchers {
	// http://stackoverflow.com/questions/15065070/implement-a-Bag-bag-as-scala-collection

	"Bag" should "add elements properly" in {
		Bag("X" -> 3, "Y" -> 1) + "X" shouldEqual Bag("X" -> 4, "Y" -> 1)
		Bag("X" -> 3, "Y" -> 1) + "Z" shouldEqual Bag("X" -> 3, "Y" -> 1, "Z" -> 1)

		Bag("X" -> 3.5, "Y" -> 1.3) + "X" shouldEqual Bag("X" -> 4.5, "Y" -> 1.3)
		Bag("X" -> 3.4, "Y" -> 1.3) + "Z" shouldEqual Bag("X" -> 3.4, "Y" -> 1.3, "Z" -> 1.0)

	}

	it should "add elements with number of occurrences properly" in {
		Bag("X" -> 3, "Y" -> 1) + ("X", 3) shouldEqual Bag("X" -> 6, "Y" -> 1)
		Bag("X" -> 3, "Y" -> 1) + ("Z", 2) shouldEqual Bag("X" -> 3, "Y" -> 1, "Z" -> 2)
		Bag("X" -> 3, "Y" -> 1) + ("Z", 0) shouldEqual Bag("X" -> 3, "Y" -> 1)

		Bag("X" -> 3.5, "Y" -> 1.3) + ("X", 1.2) shouldEqual Bag("X" -> 4.7, "Y" -> 1.3)
		Bag("X" -> 3.4, "Y" -> 1.3) + ("Z", 2.2) shouldEqual Bag("X" -> 3.4, "Y" -> 1.3, "Z" -> 2.2)
	}

	it should "remove elements properly" in {
		Bag("a" -> 2, "b" -> 5) - "b" shouldEqual Bag("a" -> 2, "b" -> 4)
		Bag("a" -> 2, "b" -> 5) - "c" shouldEqual Bag("a" -> 2, "b" -> 5)

		Bag("X" -> 3.5, "Y" -> 1.3) - "X" shouldEqual Bag("X" -> 2.5, "Y" -> 1.3)
		Bag("X" -> 3.4, "Y" -> 1.3) - "Z" shouldEqual Bag("X" -> 3.4, "Y" -> 1.3)
	}

	it should "remove elements with number of occurrences properly" in {
		Bag("X" -> 3, "Y" -> 1) - ("X", 2) shouldEqual Bag("X" -> 1, "Y" -> 1)
		Bag("X" -> 3, "Y" -> 1) - ("Y", 2) shouldEqual Bag("X" -> 3)
		Bag("X" -> 3, "Y" -> 1) - ("Z", 0) shouldEqual Bag("X" -> 3, "Y" -> 1)

		Bag("X" -> 3.5, "Y" -> 1.3) - ("X", 2.6) shouldEqual Bag("X" -> 0.9, "Y" -> 1.3)
		Bag("X" -> 3.5, "Y" -> 1.3) - ("Y", 1.4) shouldEqual Bag("X" -> 3.5)
		Bag("X" -> 3.4, "Y" -> 1.3) - ("Z", 2.2) shouldEqual Bag("X" -> 3.4, "Y" -> 1.3)
	}

	"Bag sum" should "be correct" in {
		val res1 = Bag(100 -> 3, 10 -> 2, 1 -> 7)
		val res2 = Bag("X" -> 5, "Y" -> 4, "Z" -> 1)
		val res3 = Bag("X" -> 5.7, "Y" -> 4.3, "Z" -> 1.3)
		Bag(10 -> 1, 100 -> 3) ++ Bag(10 -> 1, 1 -> 7) shouldEqual res1
		Bag("X" -> 3, "Y" -> 1) ++ Bag("X" -> 2, "Y" -> 3, "Z" -> 1) shouldEqual res2
		Bag("X" -> 3.4, "Y" -> 1.3) ++ Bag("X" -> 2.3, "Y" -> 3.0, "Z" -> 1.3) shouldEqual res3
	}

	"Bag difference" should "be correct" in {
		val res1 = Bag("b" -> 5)
		val res2 = Bag("X" -> 1, "Y" -> 1)
		val res3 = Bag("X" -> 0.6, "Y" -> 1.2)
		val res4 = Bag("X" -> 1.6)
		Bag("a" -> 2, "b" -> 5) -- Bag("a" -> 3) shouldEqual res1
		Bag("X" -> 3, "Y" -> 1) -- Bag("X" -> 2, "Z" -> 1) shouldEqual res2
		Bag("X" -> 3.4, "Y" -> 1.3) -- Bag("X" -> 2.8, "Y" -> 0.1, "Z" -> 0.5) shouldEqual res3
		Bag("X" -> 3.4, "Y" -> 1.3) -- Bag("X" -> 1.8, "Y" -> 1.4, "Z" -> 1.3) shouldEqual res4
	}

	"Bag normalisation" should "be correct" in {
		val res1 = Bag("b" -> 1.0)
		val res2 = Bag("X" -> 0.5, "Y" -> 1.0)
		val res3 = Bag("X" -> 0.5, "Y" -> 1.0)
		val res4 = Bag("X" -> 1.0, "Y" -> 0.5555555555555556, "Z" -> 0.3333333333333333)
		Bag("b" -> 5).normalise shouldEqual res1
		Bag("X" -> 1, "Y" -> 2).normalise shouldEqual res2
		Bag("X" -> 0.6, "Y" -> 1.2).normalise shouldEqual res3
		Bag("X" -> 9.0, "Y" -> 5.0, "Z" -> 3.0).normalise shouldEqual res4
	}
}
