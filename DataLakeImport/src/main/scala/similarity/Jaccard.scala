package DataLake

import com.rockymadden.stringmetric.similarity.JaccardMetric

/**
  * A hybrid similarity measure comparing strings corresponding to the jaccard algorithm
  */
object Jaccard extends SimilarityMeasure[String] {
	/**
	  * Calculates the jaccard similarity score for two strings
	  * @param s string to be compared to t
	  * @param t string to be compared to s
	  * @return a normalized similarity score between 1.0 and 0.0 or
	  * the default value 0.0 if one of the input strings is empty
	  */

	override def compare(s: String, t: String) : Double = {
		val score = JaccardMetric(1).compare(s,t)
		score.getOrElse(0.0)
	}
}
