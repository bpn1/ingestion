package DataLake

import org.simmetrics.StringMetric
import org.simmetrics.metrics.CosineSimilarity

/**
  * A hybrid similarity measure comparing strings corresponding to the Soft tf-idf algorithm
  */
object SimMeasureJaccard extends SimilarityMeasure[String] {
  /**
    * Calculates the Soft tf-idf similarity score for two strings
    * @param s string to be compared to t
    * @param t string to be compared to s
    * @return a normalized similarity score between 1.0 and 0.0
    */
  override def compare(s: String, t: String) = 0.0 {

    String a = "Chilpéric II son of Childeric II";
    String b = "chilperic ii son of childeric ii";

    StringMetric metric = new Jaccard();

    return metric.compare(a, b); // 0.7812
  }
}
