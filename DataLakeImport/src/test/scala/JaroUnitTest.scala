package DataLake

import org.scalatest.FlatSpec

class JaroUnitTest extends FlatSpec {

  "score" should "return the Jaro score for given strings" in {

    val s1 = "dwayne"
    val t1 = "duane"
    val score1 = {
      Jaro.compare(s1, t1)
    }
    assert(score1 === 0.8222222222222223)

    val s2 = "jones"
    val t2 = "johnson"
    val score2 = {
      Jaro.compare(s2, t2)
    }
    assert(score2 === 0.7904761904761904)

    val s3 = "fvie"
    val t3 = "ten"
    val score3 = {
      Jaro.compare(s3, t3)
    }
    assert(score3 === 0.0)

  }
}
