package de.hpi.ingestion.deduplication.similarity

import java.net.URL


/**
  * Computes the similarity of two Hostnames from two given urls
  */
object UrlCompare extends SimilarityMeasure[String] {


	/**
	  * Extracts the hosts name out of a given url string. A possible "www." subdomain will get stripped
	  * @param s a url string
	  * @return the urls hostname as a string
	  */
	def cleanUrlString(s: String) : String = {
		new URL(s).getHost.replaceFirst("www.","")
	}

	/**
	  * Calculates the similariity score for two hostnames extracted from given urls based on JaroWinkler
	  * @param s some url
	  * @param t another url
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
