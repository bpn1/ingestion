package de.hpi.ingestion.deduplication.similarity

import com.rockymadden.stringmetric.similarity.JaroWinklerMetric

/**
  * A hybrid similarity measure comparing strings corresponding to the JaroWinkler algorithm
  */
object JaroWinkler extends SimilarityMeasure[String] {

	/**
	  * Calculates the jaccard similarity score for two strings
	  * @param s string to be compared to t
	  * @param t string to be compared to s
	  * @param u has no specific use in here
	  * @return a normalized similarity score between 1.0 and 0.0 or
	  * 0.0 as default value if one of the input strings is empty
	  */
	override def compare(s: String, t: String, u: Int = 1) : Double = {
		val score = JaroWinklerMetric.compare(s,t)
		score.getOrElse(0.0)
	}
}
