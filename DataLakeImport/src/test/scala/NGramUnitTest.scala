package DataLake

import org.scalatest.FlatSpec

class NGramUnitTest extends FlatSpec {

  "score" should "return the NGram score for given strings" in {

    val s1 = "nacht"
    val t1 = "night"
    val r1 = 0.6
    val score1 = {
      NGram.compare(s1, t1)
    }
    assert(score1 === r1)

  }
}
