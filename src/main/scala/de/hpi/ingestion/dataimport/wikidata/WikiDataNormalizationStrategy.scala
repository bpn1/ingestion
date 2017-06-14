package de.hpi.ingestion.dataimport.wikidata

import de.hpi.ingestion.implicits.RegexImplicits._
import de.hpi.ingestion.dataimport.{CountryISO3166Mapping, SharedNormalizations}

/**
  * Strategies for the normalization of WikiData entities
  */
object WikiDataNormalizationStrategy extends Serializable {
	/**
	  * Normalizes coordinates: e.g. "-1,1"
	  * @param values coordinates list
	  * @return normalized coordinates list
	  */
	def normalizeCoords(values: List[String]): List[String] = {
		values.flatMap {
			case r"""([-+]?[0-9]+\.?[0-9]*)$lat;([-+]?[0-9]+\.?[0-9]*)$long""" => List(s"$lat;$long")
			case _ => None
		}.distinct
	}

	/**
	  * Normalizes cities: e.g. "Q30", "Berlin"
	  * @param values city list
	  * @return normalized cities
	  */
	def normalizeCity(values: List[String]): List[String] = {
		values.flatMap {
			case r"""([A-Za-zÄäÖöÜüß\-_ ]*)$country""" => List(country)
			case _ => None
		}.map(_.replaceAll("(_|-)", " ")).distinct
	}

	/**
	  * Normalizes countries: e.g. "Q30", "Vereinigte Staaten"
	  * @param values country list
	  * @return normalized countries
	  */
	def normalizeCountry(values: List[String]): List[String] = {
		values.flatMap {
			case r"""([A-Za-zÄäÖöÜüß\-_ ]*)$country""" => CountryISO3166Mapping.mapping.get(country)
			case _ => None
		}.map(_.replaceAll("(_|-)", " ")).distinct
	}

	/**
	  * Normalizes sector
	  * @param values sector list
	  * @return normalized sectors
	  */
	def normalizeSector(values: List[String]): List[String] = {
		values.flatMap {
			case r"""([A-Za-zÄäÖöÜüß\-_]*)$sector""" => List(sector)
			case _ => None
		}.distinct
	}

	/**
	  * Normalizes employees
	  * @param values list of employees
	  * @return normalized employees
	  */
	def normalizeEmployees(values: List[String]): List[String] = {
		values.flatMap {
			case r"""\+(\d+)${employees};1""" => List(employees)
			case _ => None
		}.distinct
	}

	/**
	  * Normalize urls
	  * @param values list of urls
	  * @return valid urls
	  */
	def normalizeURLs(values: List[String]): List[String] = {
		values.filter(SharedNormalizations.isValidUrl)
	}

	/***
	  * Normalize Legal Form to its abbreviation by using the legalFormMapping Map
	  * @param values list of legal forms
	  * @return normalized legal forms
	  */
	def normalizeLegalForm(values: List[String]): List[String] = {
		values.flatMap(value => List(legalFormMapping.getOrElse(value, value)))
	}

	val legalFormMapping = Map(
		"Aktiengesellschaft" -> "AG",
		"Gesellschaft mit beschränkter Haftung" -> "GmbH",
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
		"Kommanditaktiengesellschaft" -> "KGaA"
	)

	/**
	  * Normalizes all other values by default dashes
	  * @param values Strings to be normalized
	  * @return normalized strings
	  */
	def normalizeDefault(values: List[String]): List[String] = {
		values.map(_.replaceAll("(_|-)", " ")).distinct
	}

	/**
	  * Chooses the right normalization method
	  * @param attribute Attribute to be normalized
	  * @return Normalization method
	  */
	def apply(attribute: String): (List[String]) => List[String] = {
		attribute match {
			case "gen_sectors" => this.normalizeSector
			case "geo_coords" => this.normalizeCoords
			case "geo_country" => this.normalizeCountry
			case "geo_city" => this.normalizeCity
			case "gen_employees" => this.normalizeEmployees
			case "gen_urls" => this.normalizeURLs
			case "gen_legal_form" => this.normalizeLegalForm
			case _ => this.normalizeDefault
		}
	}
}
