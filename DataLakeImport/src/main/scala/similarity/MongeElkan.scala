package DataLake

import com.rockymadden.stringmetric.similarity.JaroWinklerMetric

/**
	* A hybrid similarity measure comparing strings corresponding to the MongeElkan algorithm
	*/
object MongeElkan extends SimilarityMeasure[String] {
	/**
		* Finds the highest similarity score using JaroWinkler compare for a token with a list of tokens
		* @param token token to find the highest score for
		* @param list list of tokens the token is compared to
		* @return only the highest of the scores for all combinations of token x list of tokens
		*/
	def maxSim(token: String, list: List[String]) : Double = {
		list
			.map(x => JaroWinklerMetric.compare(token, x).getOrElse(0.0))
		  .max
	}

	/**
		* Calculates the MongeElkan similarity score for two strings
		* @param s string to be compared to t
		* @param t string to be compared to s
		* @return a normalized similarity score between 1.0 and 0.0
		*/
	override def compare(s: String, t: String) : Double = {
		val x = s.split(" ").toList
		val y = t.split(" ").toList
		val sum = x
		  .map(maxSim(_, y))
		  .foldLeft(0.0)((b, a) => b+a)
		sum / y.length
	}
}
