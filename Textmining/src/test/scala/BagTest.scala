import org.scalatest.FlatSpec

class BagTest extends FlatSpec {
	// http://stackoverflow.com/questions/15065070/implement-a-Bag-bag-as-scala-collection
	"Bag" should "add elements properly" in {
		assert((Bag("X" -> 3, "Y" -> 1) + "X").equals(
			Bag("X" -> 4, "Y" -> 1)))
		assert((Bag("X" -> 3, "Y" -> 1) + "Z").equals(
			Bag("X" -> 3, "Y" -> 1, "Z" -> 1)))

		assert((Bag("X" -> 2.8, "Y" -> 0.7) + "X").equals(
			Bag("X" -> 3.8, "Y" -> 0.7)))
		assert((Bag("X" -> 5.2, "Y" -> 3) + "Z").equals(
			Bag("X" -> 5.2, "Y" -> 3, "Z" -> 1)))
	}

	"Bag" should "remove elements properly" in {
		assert((Bag("a" -> 2, "b" -> 5) - "b").equals(
			Bag("a" -> 2, "b" -> 4)))
		assert((Bag("a" -> 2, "b" -> 5) - "c").equals(
			Bag("a" -> 2, "b" -> 5)))

		assert((Bag("a" -> 2.6, "b" -> 5.1) - "b").equals(
			Bag("a" -> 2.6, "b" -> 4.1)))
		assert((Bag("a" -> 2.89, "b" -> 5) - "c").equals(
			Bag("a" -> 2.89, "b" -> 5)))
	}

	//	"Bag sum" should "be correct" in {
	//		assert((Bag(10 -> 1, 100 -> 3) ++ Bag(10 -> 1, 1 -> 7)).equals(
	//			Bag(100 -> 3, 10 -> 2, 1 -> 7)))
	//	}
	//
	//	"Bag difference" should "be correct" in {
	//		assert((Bag("a" -> 2, "b" -> 5) -- Bag("a" -> 3)).equals(
	//			Bag("b" -> 5)))
	//	}
}