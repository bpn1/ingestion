package de.hpi.ingestion.textmining.models

import org.scalatest.{FlatSpec, Matchers}

class BagTest extends FlatSpec with Matchers {

	"Bag" should "add elements properly" in {
		Bag("X" -> 3, "Y" -> 1) + "X" shouldEqual Bag("X" -> 4, "Y" -> 1)
		Bag("X" -> 3, "Y" -> 1) + "Z" shouldEqual Bag("X" -> 3, "Y" -> 1, "Z" -> 1)

		Bag("X" -> 3.5, "Y" -> 1.3) + "X" shouldEqual Bag("X" -> 4.5, "Y" -> 1.3)
		Bag("X" -> 3.4, "Y" -> 1.3) + "Z" shouldEqual Bag("X" -> 3.4, "Y" -> 1.3, "Z" -> 1.0)
	}

	it should "add elements with number of occurrences properly" in {
		Bag("X" -> 3, "Y" -> 1) + ("X", 3) shouldEqual Bag("X" -> 6, "Y" -> 1)
		Bag("X" -> 3, "Y" -> 1) + ("X", -3) shouldEqual Bag("Y" -> 1)
		Bag("X" -> 3, "Y" -> 1) + ("Z", 2) shouldEqual Bag("X" -> 3, "Y" -> 1, "Z" -> 2)
		Bag("X" -> 3, "Y" -> 1) + ("Z", 0) shouldEqual Bag("X" -> 3, "Y" -> 1)

		Bag("X" -> 3.5, "Y" -> 1.3) + ("X", 1.2) shouldEqual Bag("X" -> 4.7, "Y" -> 1.3)
		Bag("X" -> 3.4, "Y" -> 1.3) + ("Z", 2.2) shouldEqual Bag("X" -> 3.4, "Y" -> 1.3, "Z" -> 2.2)
		Bag("X" -> 3.4, "Y" -> 1.3) + ("Z", -2.2) shouldEqual Bag("X" -> 3.4, "Y" -> 1.3)
		Bag("X" -> 3.4, "Y" -> 1.3) + ("Y", -1.0) shouldEqual Bag("X" -> 3.4, "Y" -> 0.3)
	}

	it should "remove elements properly" in {
		Bag("a" -> 2, "b" -> 5) - "b" shouldEqual Bag("a" -> 2, "b" -> 4)
		Bag("a" -> 2, "b" -> 5) - "c" shouldEqual Bag("a" -> 2, "b" -> 5)
		Bag("a" -> 2, "b" -> 1) - "b" shouldEqual Bag("a" -> 2)

		Bag("X" -> 3.5, "Y" -> 1.3) - "X" shouldEqual Bag("X" -> 2.5, "Y" -> 1.3)
		Bag("X" -> 3.4, "Y" -> 1.3) - "Z" shouldEqual Bag("X" -> 3.4, "Y" -> 1.3)
		Bag("X" -> 3.4, "Y" -> 0.7) - "Y" shouldEqual Bag("X" -> 3.4)
	}

	it should "remove elements with number of occurrences properly" in {
		Bag("X" -> 3, "Y" -> 1) - ("X", 2) shouldEqual Bag("X" -> 1, "Y" -> 1)
		Bag("X" -> 3, "Y" -> 1) - ("Y", 2) shouldEqual Bag("X" -> 3)
		Bag("X" -> 3, "Y" -> 1) - ("Y", -2) shouldEqual Bag("X" -> 3, "Y" -> 3)
		Bag("X" -> 3, "Y" -> 1) - ("Z", 0) shouldEqual Bag("X" -> 3, "Y" -> 1)

		Bag("X" -> 3.5, "Y" -> 1.3) - ("X", 2.6) shouldEqual Bag("X" -> 0.9, "Y" -> 1.3)
		Bag("X" -> 3.5, "Y" -> 1.3) - ("Y", 1.4) shouldEqual Bag("X" -> 3.5)
		Bag("X" -> 3.5, "Y" -> 1.3) - ("Y", -1.4) shouldEqual Bag("X" -> 3.5, "Y" -> 2.7)
		Bag("X" -> 3.4, "Y" -> 1.3) - ("Z", 2.2) shouldEqual Bag("X" -> 3.4, "Y" -> 1.3)
		Bag("X" -> 3.4, "Y" -> 1.3) - ("Z", -2.2) shouldEqual Bag("X" -> 3.4, "Y" -> 1.3, "Z" -> 2.2)
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

	"Bag iterator" should "iterate over all full elements" in {
		val intBag = Bag("A" -> 2, "B" -> 1)
		intBag.iterator.toList shouldEqual List("A", "A", "B")
		val doubleBag = Bag("A" -> 2.3, "B" -> 1.3, "C" -> 0.3)
		doubleBag.iterator.toList shouldEqual List("A", "A", "B")
	}

	"Element size" should "be the integer value" in {
		val intBag = Bag("A" -> 1)
		intBag.elementSize(0) shouldEqual 0
		intBag.elementSize(1) shouldEqual 1
		intBag.elementSize(2) shouldEqual 2

		val doubleBag = Bag("A" -> 1.0)
		doubleBag.elementSize(0.0) shouldEqual 0
		doubleBag.elementSize(0.5) shouldEqual 0
		doubleBag.elementSize(1.0) shouldEqual 1
		doubleBag.elementSize(2.3) shouldEqual 2
	}

	"Bag elements" should "be found" in {
		val bag = Bag("A" -> 2, "B" -> 1, "C" -> 1)
		bag.contains("A") shouldBe true
		bag.contains("E") shouldBe false
	}

	"Equal Bags" should "be equal and have the same hash code" in {
		val bag1 = Bag[String, Int]("A", "B", "C", "A")
		val bag2 = Bag("A" -> 2, "B" -> 1, "C" -> 1)
		val bag3 = Bag[String, Int]("C", "D")
		bag1 shouldEqual bag2
		bag1 should not equal bag3
		bag1.hashCode() shouldEqual bag2.hashCode()
		bag1.hashCode() should not equal bag3.hashCode()
	}

	"Bag as string" should "equal a Map as string" in {
		val bag = Bag("A" -> 2, "B" -> 1, "C" -> 1)
		val expected = "Bag(A -> 2, B -> 1, C -> 1)"
		bag.toString shouldEqual expected
	}

	"Empty" should "return empty bags" in {
		val bag = Bag("A" -> 1)
		bag.empty.isEmpty shouldBe true
		Bag.empty[String, Int].isEmpty shouldBe true
		Bag.empty[String, Double].isEmpty shouldBe true
	}

	"Bag builder" should "build Integer Bags properly" in {
		val builder = Bag.empty[String, Int].newBuilder
		builder += "A"
		builder += "B"
		builder += "C"
		builder += "A"
		val bag1 = builder.result()
		val res1 = Bag("A" -> 2, "B" -> 1, "C" -> 1)
		builder.clear()
		builder += "C"
		builder += "E"
		builder += "D"
		builder += "D"
		val bag2 = builder.result()
		val res2 = Bag("C" -> 1, "D" -> 2, "E" -> 1)
		bag1 shouldEqual res1
		bag2 shouldEqual res2
	}

	it should "build Double Bags properly" in {
		val builder = Bag.empty[String, Double].newBuilder
		builder += "A"
		builder += "B"
		builder += "C"
		builder += "A"
		val bag1 = builder.result()
		val res1 = Bag("A" -> 2.0, "B" -> 1.0, "C" -> 1.0)
		builder.clear()
		builder += "C"
		builder += "E"
		builder += "D"
		builder += "D"
		val bag2 = builder.result()
		val res2 = Bag("C" -> 1.0, "D" -> 2.0, "E" -> 1.0)
		bag1 shouldEqual res1
		bag2 shouldEqual res2
	}
}
