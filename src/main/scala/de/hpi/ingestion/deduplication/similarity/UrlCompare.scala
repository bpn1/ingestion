package de.hpi.ingestion.deduplication.similarity

import java.net.URL


/**
  * Computes the similarity of two Hostnames from two given URLs
  */
object UrlCompare extends SimilarityMeasure[String] {


	def cleanUrlString(s: String) : String = {
		val test = new URL(s).getHost.replaceFirst("www.","")
		println(test)
		test
	}

	/**
	  * Calculates the similariity score for two hostnames extracted from given URLs based on JaroWinkler
	  * @param s some URL
	  * @param t another URL
	  * @param u no specific use in here
	  * @return a normalized similarity score between 1.0 and 0.0 or
	  * 0.0 as default value if one of the input strings is empty
	  */
	override def compare(s: String, t: String, u: Int = 1) : Double = {
		JaroWinkler.compare(
			cleanUrlString(s),
			cleanUrlString(t)
		)
	}
}
