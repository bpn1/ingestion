package DataLake

import org.scalatest.FlatSpec

class JaroWinklerUnitTest extends FlatSpec {

  "score" should "return the JaroWinkler score for given strings" in {

    val s1 = "dwayne"
    val t1 = "duane"
    val score1 = {
      JaroWinkler.compare(s1, t1)
    }
    assert(score1 === 0.8400000000000001)

    val s2 = "jones"
    val t2 = "johnson"
    val score2 = {
      JaroWinkler.compare(s2, t2)
    }
    assert(score2 === 0.8323809523809523)

    val s3 = "fvie"
    val t3 = "ten"
    val score3 = {
      JaroWinkler.compare(s3, t3)
    }
    assert(score3 === 0.0)

  }
}
