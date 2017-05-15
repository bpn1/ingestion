package de.hpi.ingestion.dataimport.dbpedia

import de.hpi.ingestion.dataimport.NormalizationStrategy
import de.hpi.ingestion.implicits.RegexImplicits._

/**
  * Strategies for the normalization of DBPedia entities
  */
object DBpediaNormalizationStrategy extends NormalizationStrategy("categorization_dbpedia.xml") with Serializable {
	mapping = this.parseNormalizationConfig()

	/**
	  * Normalizes Employees
	  * @param values employees list
	  * @return normalizes employees
	  */
	def normalizeEmployees(values: List[String]): List[String] = {
		values.flatMap {
			case r"""([-+]?[0-9]+\.?[0-9]*)${employees}\^\^xsd:integer""" => List(employees)
			case r"""([-+]?[0-9]+\.?[0-9]*)${employees}\^\^xsd:nonNegativeInteger""" => List(employees)
			case r"""über (\d+)${employees}@de \.""" => List(employees)
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
	  * Normalizes sector
	  * @param values sector list
	  * @return normalized sectors
	  */
	def normalizeSector(values: List[String]): List[String] = {
		values.flatMap {
			case r"""dbpedia-de:([A-Za-zÄäÖöÜüß\-_]+)${sector}""" => this.mapSector(sector)
			case _ => None
		}
	}

	/**
	  * Normalizes cities
	  * @param values city list
	  * @return normalized cities
	  */
	def normalizeCity(values: List[String]): List[String] = {
		values.flatMap {
			case r"""dbpedia-de:(.+)${city}""" => List(city)
			case r"""(.+)${city}@de \.""" => List(city)
			case other => List(other)
		}.distinct
	}

	/**
	  * Chooses the right normalization method
	  * @param attribute Attribute to be normalized
	  * @return Normalization method
	  */
	def apply(attribute: String): (List[String]) => List[String] = {
		attribute match {
			case "gen_sector" => this.normalizeSector
			case "gen_employees" => this.normalizeEmployees
			case "geo_country" => this.normalizeCountry
			case "geo_coords" => this.normalizeCoords
			case "geo_city" => this.normalizeCity
			case _ => identity
		}
	}
}
