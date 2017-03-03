package DataLake

import org.scalatest.{FlatSpec, Matchers}

class OverlapUnitTest extends FlatSpec with Matchers {

	"compare" should "return the Overlap score for given strings" in {
		val s1 = "nacht"
		val t1 = "night"
		val r1 = 0.6
		val score1 = Overlap.compare(s1, t1)
		score1 shouldEqual r1
	}
}
