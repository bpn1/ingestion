package DataLake

import org.scalatest.FlatSpec

class DiceSorensenUnitTest extends FlatSpec {

  "score" should "return the DiceSorensen score for given strings" in {

    val s1 = "night"
    val t1 = "nachts"
    val score1 = {
      DiceSorensen.compare(s1, t1)
    }
    assert(score1 === 0.5454545454545454)

    val s2 = "context"
    val t2 = "contact"
    val score2 = {
      DiceSorensen.compare(s2, t2)
    }
    assert(score2 === 0.7142857142857143)

  }
}
