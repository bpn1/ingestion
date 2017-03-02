import org.scalatest.FlatSpec

class BagTest extends FlatSpec {
	// http://stackoverflow.com/questions/15065070/implement-a-Bag-bag-as-scala-collection
	"Bag" should "add elements properly" in {
		assert(Bag("X" -> 3, "Y" -> 1) + "X" ===
			Bag("X" -> 4, "Y" -> 1))
		assert(Bag("X" -> 3, "Y" -> 1) + "Z" ===
			Bag("X" -> 3, "Y" -> 1, "Z" -> 1))
	}

	it should "remove elements properly" in {
		assert(Bag("a" -> 2, "b" -> 5) - "b" ===
			Bag("a" -> 2, "b" -> 4))
		assert(Bag("a" -> 2, "b" -> 5) - "c" ===
			Bag("a" -> 2, "b" -> 5))
	}

	"Bag sum" should "be correct" in {
		assert(Bag(10 -> 1, 100 -> 3) ++ Bag(10 -> 1, 1 -> 7) ===
			Bag(100 -> 3, 10 -> 2, 1 -> 7))
	}

	"Bag difference" should "be correct" in {
		assert(Bag("a" -> 2, "b" -> 5) -- Bag("a" -> 3) ===
			Bag("b" -> 5))
	}
}
