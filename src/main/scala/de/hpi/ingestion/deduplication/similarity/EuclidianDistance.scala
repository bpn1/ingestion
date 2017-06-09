package de.hpi.ingestion.deduplication.similarity

import java.lang.Math.sqrt

/**
  * A similarity measure to classify the distance of two points into a score
  */
object EuclidianDistance extends SimilarityMeasure[String] {

	/**
	  * Computes a metric (km) distance from twi given geographical points according to
	  * http://jonisalonen.com/2014/computing-distance-between-coordinates-can-be-simple-and-fast/
	  * @param lat1 X-coord of geographical point 1
	  * @param lng1 Y-coord of geographical point 1
	  * @param lat2 X-coord of geographical point 2
	  * @param lng2 Y-coord of geographical point 2
	  * @return a metric (km) distance
	  */
	def computeDistance(lat1: Double, lng1: Double, lat2: Double, lng2: Double) : Double = {
			val degreeLength = 110.25
			val x = lat2 - lat1
			val y = (lng2 - lng1)* Math.cos(lat1)
			degreeLength * sqrt(x * x + y * y)
	}

	/**
	  * Classifies the computed Distance into a similarity score regarding to a given scale factor
	  * @param distance a geographical distance in kilometers
	  * @param scale a scale up factor for the hardcoded distance thresholds
	  * @return a similarity score
	  */
	def turnDistanceIntoScore(distance: Double, scale: Int = 1): Double = {
			distance match {
				case x if x <= (5.0 * scale) => 1.0
				case x if x <= (20.0 * scale) => 0.75
				case x if x <= (100.0 * scale) => 0.5
				case _ => 0.0
			}
	}

	/**
	  * Calculates the euclidian distance for two given geocodes
	  * @param s geocode as List(lat,long) to be compared to t
	  * @param t geocode as List(lat,long) to be compared to s
	  * @param u can scale up the threshold distances
	  * @return a normalized similarity score between 1.0 and 0.0 or
	  * 0.0 as default value if one of the input strings is empty
	  */
	override def compare(s: String, t: String, u: Int = 1) : Double = {
		val Array(lat1, lng1) = s.split(";").map(_.toDouble)
		val Array(lat2, lng2) = t.split(";").map(_.toDouble)
		val distance = computeDistance(lat1, lng1, lat2, lng2)
		turnDistanceIntoScore(distance,u)
	}
}
