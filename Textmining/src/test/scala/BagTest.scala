import org.scalatest.{FlatSpec, Matchers}

class BagTest extends FlatSpec with Matchers {
	// http://stackoverflow.com/questions/15065070/implement-a-Bag-bag-as-scala-collection

	"Bag" should "add elements properly" in {
		Bag("X" -> 3, "Y" -> 1) + "X" shouldEqual Bag("X" -> 4, "Y" -> 1)
		Bag("X" -> 3, "Y" -> 1) + "Z" shouldEqual Bag("X" -> 3, "Y" -> 1, "Z" -> 1)
	}

	it should "remove elements properly" in {
		Bag("a" -> 2, "b" -> 5) - "b" shouldEqual Bag("a" -> 2, "b" -> 4)
		Bag("a" -> 2, "b" -> 5) - "c" shouldEqual Bag("a" -> 2, "b" -> 5)
	}

	"Bag sum" should "be correct" in {
		Bag(10 -> 1, 100 -> 3) ++ Bag(10 -> 1, 1 -> 7) shouldEqual Bag(100 -> 3, 10 -> 2, 1 -> 7)
	}

	"Bag difference" should "be correct" in {
		Bag("a" -> 2, "b" -> 5) -- Bag("a" -> 3) shouldEqual Bag("b" -> 5)
	}
}
