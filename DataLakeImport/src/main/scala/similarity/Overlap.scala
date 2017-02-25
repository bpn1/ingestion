package DataLake

import com.rockymadden.stringmetric.similarity.OverlapMetric

/**
  * A hybrid similarity measure comparing strings corresponding to the Overlap algorithm
  */
object Overlap extends SimilarityMeasure[String] {
  /**
    * Calculates the Overlap similarity score for two strings
    * @param s string to be compared to t
    * @param t string to be compared to s
    * @return a normalized similarity score between 1.0 and 0.0
    */

  override def compare(s: String, t: String) : Double = {
    val score = OverlapMetric(1).compare(s,t)
    return score.getOrElse(0.0)
  }
}
