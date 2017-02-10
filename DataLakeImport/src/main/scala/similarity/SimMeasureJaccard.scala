package DataLake

import org.simmetrics.StringMetric
import org.simmetrics.metrics.CosineSimilarity

/**
  * A hybrid similarity measure comparing strings corresponding to the jaccard algorithm
  */
object SimMeasureJaccard extends SimilarityMeasure[String] {
  /**
    * Calculates the jaccard similarity score for two strings
    * @param s string to be compared to t
    * @param t string to be compared to s
    * @return a normalized similarity score between 1.0 and 0.0
    */
  override def compare(s: String, t: String) = 0.0 {

    StringMetric metric = new Jaccard();

    return metric.compare(s, t);
  }
}
