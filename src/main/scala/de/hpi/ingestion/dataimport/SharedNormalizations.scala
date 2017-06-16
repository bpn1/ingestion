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
			.map(_.replaceAll(" \\.", "."))
			.flatMap(value => List(this.legalFormMapping.getOrElse(value, value)))
			.distinct
	}

	val legalFormMapping = Map(
		// AG
		"Aktiengesellschaft" -> "AG",
		"Kleine Aktiengesellschaft" -> "AG",
		"Gemeinnützige Aktiengesellschaft" -> "AG",
		"Geschlossene Aktiengesellschaft" -> "AG",
		// GmbH
		"Gesellschaft mit beschränkter Haftung" -> "GmbH",
		"Gemeinnützige GmbH" -> "GmbH",
		"Gesellschaft mbH" -> "GmbH",
		"gesellschaft mbH" -> "GmbH",
		"GmbH gesellschaft" -> "GmbH",
		"gesellschaft GmbH" -> "GmbH",
		"GmbH." -> "GmbH",
		"gesellschaft m. b. H" -> "GmbH",
		"GmbH Gesellschaft" -> "GmbH",
		"Gesellschaft GmbH" -> "GmbH",
		". GmbH" -> "GmbH",
		"Einpersonen GmbH" -> "GmbH",
		"GmbH gesellschaft gesellschaft" -> "GmbH",
		"gesellschaft mbH." -> "GmbH",
		// gGmbH
		"gemeinnützige GmbH" -> "gGmbH",
		// GmbH & Co. KG
		"Gesellschaft mbH & Co. KG" -> "GmbH & Co. KG",
		"gesellschaft mbH & Co. KG" -> "GmbH & Co. KG",
		"gesellschaft mbH & Co KG" -> "GmbH & Co. KG",
		"GmbH & Co KG" -> "GmbH & Co. KG",
		"gesellschaft GmbH & Co. KG" -> "GmbH & Co. KG",
		"GmbH Co. KG" -> "GmbH & Co. KG",
		"GmbH Co KG" -> "GmbH & Co. KG",
		"GmbH + Co. KG" -> "GmbH & Co. KG",
		// e.V.
		"eingetragener Verein" -> "e.V.",
		"gemeinnütziger Verein" -> "e.V.",
		"Verein" -> "e.V.",
		"e.V" -> "e.V.",
		"e. V." -> "e.V.",
		"e V." -> "e.V.",
		"e V" -> "e.V.",
		"e. V" -> "e.V.",
		"gemeinschaft e. V" -> "e.V.",
		"Gesellschaft e. V." -> "e.V.",
		// e.K.
		"e. K." -> "e.K.",
		"e K." -> "e.K.",
		"e. K" -> "e.K.",
		". e. K." -> "e.K.",
		// UG haftungsbeschränkt
		"gesellschaft UG haftungsbeschränkt" -> "UG haftungsbeschränkt",
		"gemeinnützige UG haftungsbeschränkt" -> "UG haftungsbeschränkt",
		// EG
		"Eingetragene Genossenschaft" -> "EG",
		// AdöR
		"Anstalt des öffentlichen Rechts"-> "AdöR",
		// KG
		"Kommanditgesellschaft" -> "KG",
		// SE
		"Europäische Gesellschaft" -> "SE",
		// oHG
		"Offene Handelsgesellschaft" -> "oHG",
		"OHG" -> "oHG",
		// GbR
		"Gesellschaft bürgerlichen Rechts" -> "GbR",
		// KGaA
		"Kommanditgesellschaft auf Aktien" -> "KGaA",
		"Kommanditgesellschaft auf Aktien (allgemein)" -> "KGaA",
		"Kommanditaktiengesellschaft" -> "KGaA"
	)
}
