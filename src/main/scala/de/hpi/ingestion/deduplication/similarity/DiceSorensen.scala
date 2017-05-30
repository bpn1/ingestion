package de.hpi.ingestion.deduplication.similarity

import com.rockymadden.stringmetric.similarity.DiceSorensenMetric

/**
  * A hybrid similarity measure comparing strings corresponding to the jaccard algorithm
  */
object DiceSorensen extends SimilarityMeasure[String] {

	/**
	  * Calculates the jaccard similarity score for two strings
	  * @param s string to be compared to t
	  * @param t string to be compared to s
	  * @param u specifes the n-gram in the algorithm
	  * @return a normalized similarity score between 1.0 and 0.0 or
	  * the default value 0.0 if one of the input strings is empty
	  */
	override def compare(s: String, t: String, u: Int = 1) : Double = {
		val score = DiceSorensenMetric(u).compare(s,t)
		score.getOrElse(0.0)
	}
}
