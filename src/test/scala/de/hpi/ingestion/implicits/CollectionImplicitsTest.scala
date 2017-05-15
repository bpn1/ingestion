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
}
