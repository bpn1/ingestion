package de.hpi.ingestion.dataimport.wikidata

import de.hpi.ingestion.implicits.RegexImplicits._

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
	  * Normalizes countries: e.g. "Q30", "Vereinigte Staaten"
	  * @param values country list
	  * @return normalized countries
	  */
	def normalizeLocation(values: List[String]): List[String] = {
		values.flatMap {
			case r"""([A-Za-zÄäÖöÜüß\-_ ]*)$country""" => List(country)
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
			case "geo_country" | "geo_city" => this.normalizeLocation
			case "gen_employees" => this.normalizeEmployees
			case _ => this.normalizeDefault
		}
	}
}
