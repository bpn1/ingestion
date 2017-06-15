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

	/**
	  * Normalize Legal Form to its abbreviation by using the legalFormMapping Map
	  * @param values list of legal forms
	  * @return normalized legal forms
	  */
	def normalizeLegalForm(values: List[String]): List[String] = {
		values
			.flatMap(value => List(this.legalFormMapping.getOrElse(value, value).replaceAll(" \\.", ".")))
			.distinct
	}

	val legalFormMapping = Map(
		"Aktiengesellschaft" -> "AG",
		"Gesellschaft mit beschränkter Haftung" -> "GmbH",
		"Gesellschaft mbH" -> "GmbH",
		"Eingetragene Genossenschaft" -> "EG",
		"Anstalt des öffentlichen Rechts"-> "AdöR",
		"Kommanditgesellschaft" -> "KG",
		"Europäische Gesellschaft" -> "SE",
		"Offene Handelsgesellschaft" -> "oHG",
		"Gesellschaft bürgerlichen Rechts" -> "GbR",
		"eingetragener Verein" -> "e.V.",
		"gemeinnütziger Verein" -> "e.V.",
		"Gemeinnützige GmbH" -> "GmbH",
		"Kommanditgesellschaft auf Aktien" -> "KGaA",
		"Kommanditgesellschaft auf Aktien (allgemein)" -> "KGaA",
		"Einpersonen GmbH" -> "GmbH",
		"Kleine Aktiengesellschaft" -> "AG",
		"Verein" -> "e.V.",
		"Gemeinnützige Aktiengesellschaft" -> "AG",
		"Geschlossene Aktiengesellschaft" -> "AG",
		"Kommanditaktiengesellschaft" -> "KGaA",
		"e.V" -> "e.V."
	)
}
