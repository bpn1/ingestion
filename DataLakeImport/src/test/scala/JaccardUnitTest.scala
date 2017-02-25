package DataLake

import org.scalatest.FlatSpec

class JaccardUnitTest extends FlatSpec {

  "score" should "return the Jaccard score for given strings" in {

    val s1 = "apple"
    val t1 = "applet"
    val score1 = {
      Jaccard.compare(s1, t1)
    }
    assert(score1 === 0.8333333333333334)

    val s2 = "SomeText"
    val t2 = "SomeText"
    val score2 = {
      Jaccard.compare(s2, t2)
    }
    assert(score2 === 1.0)

    val s3 = "string"
    val t3 = "gnirts"
    val score3 = {
      Jaccard.compare(s3, t3)
    }
    assert(score3 === 1.0)

    val s4 = "a"
    val t4 = "b"
    val score4 = {
      Jaccard.compare(s4, t4)
    }
    assert(score4 === 0.0)

  }
}
