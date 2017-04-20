package de.hpi.ingestion.deduplication.similarity

import java.lang.Math.{pow, sqrt, cos}

/**
  * A hybrid similarity measure comparing strings corresponding to the jaccard algorithm
  */
object EuclidianDistance extends SimilarityMeasure[String] {

	 // http://jonisalonen.com/2014/computing-distance-between-coordinates-can-be-simple-and-fast/
	 def computeDistance(lat1: Double, lng1: Double, lat2: Double, lng2: Double) : Double = {
			val degreeLength = 110.25
			val x = lat2 - lat1
			val y = (lng2 - lng1)* Math.cos(lat1)
			degreeLength * sqrt(x * x + y * y)
	 }

	/**
	  * Calculates the euclidian distance for two given geocodes
	  * @param s geocode as List(lat,long) to be compared to t
	  * @param t geocode as List(lat,long) to be compared to s
	  * @param u has no specific use in here
	  * @return a normalized similarity score between 1.0 and 0.0 or
	  * 0.0 as default value if one of the input strings is empty
	  */
	override def compare(s: String, t: String, u: Int = 1) : Double = {
		 val geoPoint1 = s.toList
		 val geoPoint2 = t.toList
		 //val distance = Math.sqrt(Math.pow(geoPoint1(0)-geoPoint2(0), 2) + Math.pow(geoPoint1(1)-geoPoint2(1), 2))
		 val distance = computeDistance(geoPoint1(0),geoPoint1(1),geoPoint2(0),geoPoint2(1))
		 distance
	}
}
