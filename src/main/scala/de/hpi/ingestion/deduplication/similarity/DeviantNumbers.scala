package de.hpi.ingestion.deduplication.similarity

import java.lang.Math._


/**
  * A hybrid similarity measure comparing strings corresponding to the jaccard algorithm
  */
object DeviantNumbers  extends SimilarityMeasure[String] {

	/**
	  * Calculates how close are two given numbers to each other in terms of percentage
	  * @param s double to be compared to t
	  * @param t double to be compared to s
	  * @param u has no specific use in here
	  * @return a normalized similarity score between 1.0 and 0.0 or
	  */
	override def compare(s: String, t: String, u: Int = 1) : Double = {
		val max = Math.max(s.toDouble, t.toDouble)
		val min = Math.min(s.toDouble, t.toDouble)
		min / max
	 }
}
