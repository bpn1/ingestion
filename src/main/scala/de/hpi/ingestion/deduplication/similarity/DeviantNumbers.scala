package de.hpi.ingestion.deduplication.similarity

import scala.collection.TraversableOnce


/**
	* A hybrid similarity measure comparing strings corresponding to the jaccard algorithm
	*/
object DeviantNumbers  extends SimilarityMeasure[String] {

	// stackoverflow.com/questions/23564120/min-max-of-collections-containing-nan-handling-incomparability-in-ordering
	implicit class MinMaxNanAware(t: TraversableOnce[Double]) {
			def nanAwareMin = t.minBy(x => if (x.isNaN) Double.PositiveInfinity else x)
			def nanAwareMax = t.maxBy(x => if (x.isNaN) Double.NegativeInfinity else x)
	}

	/**
	  * Calculates the deviant similarity score for two given doubles
	  * @param s double to be compared to t
	  * @param t double to be compared to s
	  * @param u has no specific use in here
	  * @return a normalized similarity score between 1.0 and 0.0 or
	  */
	override def compare(s: String, t: String, u: Int = 1) : Double = {

			val givenValues = List(s.toDouble, t.toDouble)
			val max = givenValues.nanAwareMax
			val min = givenValues.nanAwareMin
			println("max: ", max)
			println("min: ", min)
			val quotient = min / max
			println("quotient: ", quotient)
			val score = quotient
			score
	 }
}
