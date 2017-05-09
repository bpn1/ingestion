package de.hpi.ingestion.dataimport.dbpedia

import de.hpi.ingestion.implicits.RegexImplicits._

/**
  * Strategies for the normalization of DBPedia entities
  */
object DBpediaNormalizeStrategy extends Serializable {
	val xsdNumberPatter = """([-+]?[0-9]+\.?[0-9]*)\^\^xsd:.+""".r

	/**
	  * Normalizes Employees
	  * @param values employees list
	  * @return normalizes employees
	  */
	def normalizeEmployees(values: List[String]): List[String] = {
		values.flatMap {
			case xsdNumberPatter(number) => List(number)
			case r"""über (\d+)${number}@de \.""" => List(number)
			case _ => None
		}
	}

	/**
	  * Normalizes countries
	  * @param values country list
	  * @return normalized countries
	  */
	def normalizeCountry(values: List[String]): List[String] = {
		values.flatMap {
			case r"""dbpedia-de:([A-Za-zÄäÖöÜüß\-_]+)${country}""" => List(country)
			case r"""([A-Za-zÄäÖöÜüß-]+)${country}@de \.""" => List(country)
			case _ => None
		}.map(_.replaceAll("_", " "))
	}

	/**
	  * Normalizes coordinates
	  * @param values coordinates list
	  * @return normalized coordinates list
	  */
	def normalizeCoords(values: List[String]): List[String] = {
		values.flatMap {
			case xsdNumberPatter(coordinate) => List(coordinate)
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
