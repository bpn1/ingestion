package DataLake

import com.rockymadden.stringmetric.similarity.JaccardMetric
import org.scalatest.FlatSpec

class MongeElkanUnitTest extends FlatSpec {

  "score" should "return the Jaccard score for given given strings" in {
    val s = "Chilpéric II son of Childeric II"
    val t = "chilperic ii son of childeric ii"
    val score = SimMeasureJaccard.compare(s,t)
    assert(score === JaccardMetric(1).compare(s,t))
  }
}
