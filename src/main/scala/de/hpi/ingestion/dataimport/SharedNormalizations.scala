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
			.flatMap(value => List(this.legalFormMapping.getOrElse(value, matchesLegalForm(value))))
			.distinct
	}

	/**
	  * Matches several variations of legal form
	  * @param legalForm legal form to be matched
	  * @return matched legal form or input value if input could not be matched
	  */
	def matchesLegalForm(legalForm: String): String = {
		val replacedForm = legalForm.replaceAll("(\\.| )", "")
		replacedForm match {
			case x if matchesAG(x) => "AG"
			case x if matchesGmbH(x) => "GmbH" + " & Co. KG".filter(t => matchesCoKG(x))
			case x if matchesEV(x) => "e.V."
			case x if matchesEK(x) => "e.K."
			case _ => legalForm
		}
	}

	/**
	  * Matches several variations of AG
	  * @param legalForm legal form to be matched
	  * @return boolean whether legal form matches AG
	  */
	def matchesAG(legalForm: String): Boolean = {
		val patternAG = """(?i)(aktiengesellschaft)""".r
		patternAG.findFirstIn(legalForm).isDefined
	}

	/**
	  * Matches several variations of GmbH
	  * @param legalForm legal form to be matched
	  * @return boolean whether legal form matches GmbH
	  */
	def matchesGmbH(legalForm: String): Boolean = {
		val patternGMBH = """(?i)(gmbh)""".r
		val patternMBH = """(?i)(mbh)""".r
		val patternGesellschaftGemeinschaft = """(?i)(gesellschaft|gemeinschaft)""".r
		legalForm match {
			case x if patternGMBH.findFirstIn(x).isDefined => true
			case x if patternMBH.findFirstIn(x).isDefined
				&& patternGesellschaftGemeinschaft.findFirstIn(x).isDefined => true
			case _ => false
		}
	}

	/**
	  * Matches several variations of Co KG
	  * @param legalForm legal form to be matched
	  * @return boolean whether legal form matches Co KG
	  */
	def matchesCoKG(legalForm: String): Boolean = {
		val patternCoKG = """(?i)(&?cokg)""".r
		patternCoKG.findFirstIn(legalForm).isDefined
	}

	/**
	  * Matches several variations of e.V.
	  * @param legalForm legal form to be matched
	  * @return boolean whether legal form matches e.V.
	  */
	def matchesEV(legalForm: String): Boolean = {
		val patternEV = """(?i)(ev)""".r
		patternEV.findFirstIn(legalForm).isDefined
	}

	/**
	  * Matches several variations of EK
	  * @param legalForm legal form to be matched
	  * @return boolean whether legal form matches EK
	  */
	def matchesEK(legalForm: String): Boolean = {
		val patternEK = """(?i)(ek)""".r
		patternEK.findFirstIn(legalForm).isDefined
	}

	val legalFormMapping = Map(
		// AG
		"Aktiengesellschaft" -> "AG",
		"Kleine Aktiengesellschaft" -> "AG",
		"Gemeinnützige Aktiengesellschaft" -> "AG",
		"Geschlossene Aktiengesellschaft" -> "AG",
		"Offene Aktiengesellschaft" -> "AG",
		// GmbH
		"Gesellschaft mit beschränkter Haftung" -> "GmbH",
		// e.V.
		"eingetragener Verein" -> "e.V.",
		"gemeinnütziger Verein" -> "e.V.",
		"Verein" -> "e.V.",
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
