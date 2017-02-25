package DataLake

import org.scalatest.FlatSpec

class JaccardUnitTest extends FlatSpec {

  "score" should "return the Jaccard score for given strings" in {

    val s1 = "apple"
    val t1 = "applet"
    val r1 = 0.8333333333333334
    val score1 = {
      Jaccard.compare(s1, t1)
    }
    assert(score1 === r1)

    val s2 = "SomeText"
    val t2 = "SomeText"
    val r2 = 1.0
    val score2 = {
      Jaccard.compare(s2, t2)
    }
    assert(score2 === r2)

    val s3 = "string"
    val t3 = "gnirts"
    val r3 = 1.0
    val score3 = {
      Jaccard.compare(s3, t3)
    }
    assert(score3 === r3)

    val s4 = "a"
    val t4 = "b"
    val r4 = 0.0
    val score4 = {
      Jaccard.compare(s4, t4)
    }
    assert(score4 === r4)

  }
}
