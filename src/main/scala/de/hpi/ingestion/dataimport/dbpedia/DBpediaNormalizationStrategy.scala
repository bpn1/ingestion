package de.hpi.ingestion.dataimport.dbpedia

import de.hpi.ingestion.implicits.RegexImplicits._
import de.hpi.ingestion.dataimport.CountryISO3166Mapping

/**
  * Strategies for the normalization of DBPedia entities
  */
object DBpediaNormalizationStrategy extends Serializable {
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
		}.map(_.replaceAll("(_|-)", " ")).distinct
	}

	/**
	  * Normalizes countries
	  * @param values country list
	  * @return normalized countries
	  */
	def normalizeCountry(values: List[String]): List[String] = {
		values.flatMap {
			case r"""dbpedia-de:([A-Za-zÄäÖöÜüß\-_]{3,})${country}""" => CountryISO3166Mapping.mapping.get(country)
			case r"""dbpedia-de:([A-Za-zÄäÖöÜüß\-_]{2})${country}""" => List(country)
			case r"""([A-Za-zÄäÖöÜüß\-]{3,})${country}@de \.""" => CountryISO3166Mapping.mapping.get(country)
			case r"""([A-Za-zÄäÖöÜüß\-]{2})${country}@de \.""" => List(country)
			case _ => None
		}.map(_.replaceAll("(_|-)", " ")).distinct
	}

	/**
	  * Normalizes coordinates
	  * @param values coordinates list
	  * @return normalized coordinates list
	  */
	def normalizeCoords(values: List[String]): List[String] = {
		values.flatMap {
			case r"""(\d+\.\d*)${lat}\^\^xsd:.+;(\d+\.\d*)${long}\^\^xsd:.+""" => List(s"$lat;$long")
			case _ => None
		}.distinct
	}

	/**
	  * Normalizes sector
	  * @param values sector list
	  * @return normalized sectors
	  */
	def normalizeSector(values: List[String]): List[String] = {
		values.flatMap {
			case r"""dbpedia-de:([A-Za-zÄäÖöÜüß\-_]+)${sector}""" => List(sector)
			case _ => None
		}.distinct
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
		}.map(_.replaceAll("(_|-)", " ")).distinct
	}

	/**
	  * Normalizes all other values by default (removing pre- and suffixes and dashes)
	  * @param values Strings to be normalized
	  * @return normalized strings
	  */
	def normalizeDefault(values: List[String]): List[String] = {
		values.flatMap {
			case r"""(.+)${value}\^\^xsd:.+""" => List(value)
			case r"""(.+)${value}@de \.""" => List(value)
			case r"""dbpedia-de:(.+)${value}""" => List(value)
			case other => List(other)
		}.map(_.replaceAll("(_|-)", " ")).distinct
	}

	/**
	  * Chooses the right normalization method
	  * @param attribute Attribute to be normalized
	  * @return Normalization method
	  */
	def apply(attribute: String): (List[String]) => List[String] = {
		attribute match {
			case "gen_sectors" => this.normalizeSector
			case "gen_employees" => this.normalizeEmployees
			case "geo_country" => this.normalizeCountry
			case "geo_coords" => this.normalizeCoords
			case "geo_city" => this.normalizeCity
			case _ => this.normalizeDefault
		}
	}
}
