package DataLake

import com.rockymadden.stringmetric.similarity.JaccardMetric
import org.scalatest.FlatSpec

class JaccardUnitTest extends FlatSpec {

  "score" should "return the Jaccard score for given strings" in {
    var s = "Chilpéric II son of Childeric II"
    var t = "chilperic ii son of childeric ii"

    var s = "night"
    var t = "nacht"

    var score = {
      Jaccard.compare(s, t)
    }

    assert(score === 0.35714285714285715)
  }
}
