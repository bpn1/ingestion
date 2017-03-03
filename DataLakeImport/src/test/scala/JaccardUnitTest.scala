package DataLake

import org.scalatest.{FlatSpec, Matchers}

class JaccardUnitTest extends FlatSpec with Matchers {

	"compare" should "return the Jaccard score for given strings" in {
		val testData = List(
			("apple", "applet", 0.8333333333333334),
			("SomeText", "SomeText", 1.0),
			("string", "gnirts", 1.0),
			("a", "b", 0.0))

		testData.foreach(tuple =>
			Jaccard.compare(tuple._1, tuple._2) shouldEqual tuple._3)
	}
}
