/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.implicits

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.rdd.RDD

class CollectionImplicitsTest extends FlatSpec with Matchers with SharedSparkContext {

	"Cross" should "return the cross product" in {
		val (list1, list2) = TestData.crossableLists
		val crossProduct = list1.cross(list2).toSet
		val expected = TestData.crossProduct
		crossProduct shouldEqual expected
	}

	"Asym Square" should "return this cross product" in {
		val (list1, list2) = TestData.crossableLists
		val square1 = list1.asymSquare(true)
		val expectedSquare1 = TestData.asymSquareWithReflexive
		square1.toSet shouldEqual expectedSquare1

		val square2 = list2.asymSquare()
		val expectedSquare2 = TestData.asymSquare
		square2.toSet shouldEqual expectedSquare2
	}

	"Printable set difference" should "return a diff string" in {
		val (list1, list2) = TestData.diffLists
		val diff = list1.printableSetDifference(list2)
		val expectedDiff = TestData.diffString
		diff shouldEqual expectedDiff
	}

	it should "calculate the set difference" in {
		val (list1, list2) = TestData.equalDiffLists
		val diff = list1.printableSetDifference(list2)
		val expectedDiff = TestData.equalDiffString
		diff shouldEqual expectedDiff
	}

	"Any RDD conversions" should "convert the types" in {
		val startValue = "a"
		val stringRDD = sc.parallelize(Seq(startValue))
		val anyList = List(stringRDD).toAnyRDD()
		val resultRDD = anyList.fromAnyRDD[String]().head
		val resultValue = resultRDD.first()
		anyList.head.isInstanceOf[RDD[Any]] shouldBe true
		resultRDD.isInstanceOf[RDD[String]] shouldBe true
		startValue shouldBe resultValue
	}

	"Count elements" should "count the elements" in {
		val countedData = TestData.countableData.countElements()
		val expectedCounts = TestData.countedData
		countedData shouldEqual expectedCounts
	}

	"Map keys" should "be mapped" in {
		val testMap = Map(1 -> 2, 2 -> 3, 3 -> 4)
		val mappedKeysMap = testMap.mapKeys(_ + 1)
		mappedKeysMap should not be empty
		mappedKeysMap.foreach { case (key, value) =>
			key shouldEqual value
		}
	}
}
