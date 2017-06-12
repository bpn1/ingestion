package de.hpi.ingestion.dataimport

import java.net.{MalformedURLException, URL}

/**
  * Strategies for datasource independent normalization
  */
object SharedNormalizations {
	/**
	  * Checks whether a given URL is valid
	  * @param url url to be checked
	  * @return true if url is valid and false if it is invalid
	  */
	def isValidUrl(url: String): Boolean = {
		try {
			val newURL = new URL(url)
			true
		}
		catch { case ex: MalformedURLException => false }
	}
}
