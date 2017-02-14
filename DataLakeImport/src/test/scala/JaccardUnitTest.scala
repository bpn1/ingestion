package DataLake

import com.rockymadden.stringmetric.similarity.JaccardMetric
import org.scalatest.FlatSpec

class JaccardUnitTest extends FlatSpec {

  "score" should "return the Jaccard score for given strings" in {

    val s = "night"
    val t = "nacht"

    val score = {
      Jaccard.compare(s, t)
    }

    assert(score === 0.35714285714285715)
  }
}
