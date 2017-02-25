package DataLake

import org.scalatest.FlatSpec

class OverlapUnitTest extends FlatSpec {

  "score" should "return the Overlap score for given strings" in {

    val s1 = "nacht"
    val t1 = "night"
    val score1 = {
      Overlap.compare(s1, t1)
    }
    assert(score1 === 0.6)

  }
}
