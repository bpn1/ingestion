package de.hpi.ingestion.deduplication.similarity

import java.lang.Math.{sqrt, pow}

/**
  * A hybrid similarity measure comparing strings corresponding to the jaccard algorithm
  */
object EuclidianDistance extends SimilarityMeasure[String] {

	 /**
		 * Calculates the euclidian distance score for two numbers given as strings
		 * @param s string to be compared to t
		 * @param t string to be compared to s
		 * @param u has no specific use in here
		 * @return a normalized similarity score between 1.0 and 0.0 or
		 * 0.0 as default value if one of the input strings is empty
		 */
	 override def compare(s: String, t: String, u: Int = 1) : Double = {
			val geoPoint1 = s.toList
			val geoPoint2 = t.toList
			val distance = Math.sqrt(Math.pow((geoPoint1(0)-geoPoint2(1)), 2) + Math.pow((geoPoint2(0)-geoPoint2(1)), 2))
			distance
	 }
}
