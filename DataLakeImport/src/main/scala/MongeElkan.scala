package DataLake

import com.rockymadden.stringmetric.similarity.JaroWinklerMetric

/**
	* A hybrid similarity measure comparing strings corresponding to the MongeElkan algorithm
	*/
object MongeElkan extends SimilarityMeasure[String] {

	/**
		* Finds the greatest number in a list of Doubles
		* @param list list of Doubles to be searched through
		* @param acc initial value for accumulation
		* @return the greatest number of the list, returns acc if list is empty
		*/
	def max_acc(list: List[Double], acc: Double) : Double = list match {
		case Nil => acc
		case x::xs => if (x > acc) max_acc(xs, x) else max_acc(xs, acc)
	}

	/**
		* Finds the greatest number in a list of Doubles
		* @param list list of Doubles to be searched through
		* @return the greatest number of the list, returns 0.0 if list is empty
		*/
	def max(list: List[Double]) : Double = max_acc(list, 0.0)

	/**
		* Finds the highest similarity score using JaroWinkler compare for a token with a list of tokens
		* @param token token to find the highest score for
		* @param list list of tokens the token is compared to
		* @return only the highest of the scores for all combinations of token x list of tokens
		*/
	def maxSim(token: String, list: List[String]) : Double = {
		max(list.map(x => JaroWinklerMetric.compare(token, x).getOrElse(0.0)))
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
