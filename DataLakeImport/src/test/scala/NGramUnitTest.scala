package DataLake

import org.scalatest.FlatSpec

class NGramUnitTest extends FlatSpec {

  "score" should "return the NGram score for given strings" in {

    val s1 = "nacht"
    val t1 = "night"
    val score1 = {
      NGram.compare(s1, t1)
    }
    assert(score1 === 0.6)

  }
}
