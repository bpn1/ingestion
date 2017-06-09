package de.hpi.ingestion.deduplication.similarity

import com.rockymadden.stringmetric.similarity.JaroWinklerMetric

/**
  * A hybrid similarity measure comparing strings corresponding to the MongeElkan algorithm
  */
object MongeElkan extends SimilarityMeasure[String] {

	def score(sToken: Array[String], tToken: Array[String]): Double = {
		sToken.map { token =>
			tToken.map(JaroWinklerMetric.compare(token, _).getOrElse(0.0)).max
		}.sum / sToken.length.toDouble
	}

	/**
	  * Calculates the MongeElkan similarity score for two strings
	  * @param s string to be compared to t
	  * @param t string to be compared to s
		* @param u has no specific use in here
	  * @return a normalized similarity score between 1.0 and 0.0
	  */
	override def compare(s: String, t: String, u: Int = 1) : Double = {
		val token = (s.split(" "), t.split(" "))
		val tupledScore = (score _).tupled
		List(tupledScore(token), tupledScore(token.swap)).max
	}
}
