package de.hpi.ingestion.dataimport.dbpedia

import java.net.URL
import de.hpi.ingestion.implicits.RegexImplicits._
import scala.io.Source
import scala.xml.XML

/**
  * Strategies for the normalization of DBPedia entities
  */
object DBpediaNormalizeStrategy extends Serializable {
	val mapping = this.parseNormalizationConfig(this.getClass.getResource("/categorization_dbpedia.xml"))

	def parseNormalizationConfig(url: URL): Map[String, List[String]] = {
		val xml = XML.loadString(Source
			.fromURL(url)
			.getLines()
			.mkString("\n"))

		(xml \\ "categorization" \ "category").map { attribute =>
			val key = (attribute \ "key").text
			val values = (attribute \ "mapping").map(_.text).toList
			(key, values)
		}.toMap
	}

	/**
	  * Mapping Sector to the wz2008 companion
	  * @param sector sector to map
	  * @return corresponding wz2008 entries
	  */
	def mapSector(sector: String): List[String] = this.mapping.getOrElse(sector, List(sector))
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
	  * Chooses the right normalization method
	  * @param attribute Attribute to be normalized
	  * @return Normalization method
	  */
	def apply(attribute: String): (List[String]) => List[String] = {
		attribute match {
			case "gen_sector" => this.normalizeSector
			case "gen_employees" => normalizeEmployees
			case "geo_country" => normalizeCountry
			case "geo_coords" => normalizeCoords
			case _ => identity
		}
	}
}
