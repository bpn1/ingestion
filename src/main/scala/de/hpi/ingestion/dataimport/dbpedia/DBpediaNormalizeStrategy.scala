package de.hpi.ingestion.dataimport.dbpedia

import de.hpi.ingestion.implicits.RegexImplicits._

/**
  * Strategies for the normalization of DBPedia entities
  */
object DBpediaNormalizeStrategy extends Serializable {
	/**
	  * Normalizes Employees
	  * @param values employees list
	  * @return normalizes employees
	  */
	def normalizeEmployees(values: List[String]): List[String] = {
		values.flatMap {
			case r"""([-+]?[0-9]+\.?[0-9]*)${number}\^\^xsd:integer""" => List(number)
			case r"""([-+]?[0-9]+\.?[0-9]*)${number}\^\^xsd:nonNegativeInteger""" => List(number)
			case r"""über (\d+)${number}@de \.""" => List(number)
			case _ => None
		}.distinct
	}

	/**
	  * Normalizes countries
	  * @param values country list
	  * @return normalized countries
	  */
	def normalizeCountry(values: List[String]): List[String] = {
		values.flatMap {
			case r"""dbpedia-de:([A-Za-zÄäÖöÜüß\-_]{3,})${country}""" => List(country)
			case r"""([A-Za-zÄäÖöÜüß-]{3,})${country}@de \.""" => List(country)
			case _ => None
		}.map(_.replaceAll("_", " ")).distinct
	}

	/**
	  * Normalizes coordinates
	  * @param values coordinates list
	  * @return normalized coordinates list
	  */
	def normalizeCoords(values: List[String]): List[String] = {
		values.flatMap {
			case r"""([-+]?[0-9]+\.?[0-9]*)${coordinate}\^\^xsd:float""" => List(coordinate)
			case r"""([-+]?[0-9]+\.?[0-9]*)${coordinate}\^\^xsd:double""" => List(coordinate)
			case _ => None
		}.grouped(2).toList.distinct.flatten
	}

	/**
	  * Chooses the right normalization method
	  * @param attribute Attribute to be normalized
	  * @return Normalization method
	  */
	def apply(attribute: String): (List[String]) => List[String] = {
		attribute match {
			case "gen_employees" => normalizeEmployees
			case "geo_country" => normalizeCountry
			case "geo_coords" => normalizeCoords
			case _ => identity
		}
	}
}
