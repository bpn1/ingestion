package de.hpi.ingestion.dataimport.wikidata

import de.hpi.ingestion.implicits.RegexImplicits._

/**
  * Strategies for the normalization of WikiData entities
  */
object WikiDataNormalizeStrategy {
	/**
	  * Normalizes coordinates: e.g. "-1,1"
	  * @param values coordinates list
	  * @return normalized coordinates list
	  */
	def normalizeCoords(values: List[String]): List[String] = {
		values.flatMap {
			case r"""([-+]?[0-9]+\.?[0-9]*)${lat};([-+]?[0-9]+\.?[0-9]*)${long}""" => List(lat, long)
			case _ => None
		}
	}

	/**
	  * Normalizes countries: e.g. "Q30", "Vereinigte Staaten"
	  * @param values country list
	  * @return normalized countries
	  */
	def normalizeCountry(values: List[String]): List[String] = {
		values.flatMap {
			case r"Q[0-9]+" => None
			case other => List(other)
		}
	}

	/**
	  * Chooses the right normalization method
	  * @param attribute Attribute to be normalized
	  * @return Normalization method
	  */
	def apply(attribute: String): (List[String]) => List[String] = {
		attribute match {
			case "geo_coords" => this.normalizeCoords
			case "geo_country" => this.normalizeCountry
			case _ => identity
		}
	}
}
