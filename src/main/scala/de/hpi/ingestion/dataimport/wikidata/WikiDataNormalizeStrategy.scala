package de.hpi.ingestion.dataimport.wikidata

import java.net.URL
import de.hpi.ingestion.implicits.RegexImplicits._
import scala.io.Source
import scala.xml.XML

/**
  * Strategies for the normalization of WikiData entities
  */
object WikiDataNormalizeStrategy {
	val mapping = this.parseNormalizationConfig(this.getClass.getResource("/categorization_wikidata.xml"))

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
	  * Normalizes sector
	  * @param values sector list
	  * @return normalized sectors
	  */
	def normalizeSector(values: List[String]): List[String] = {
		values.flatMap {
			case r"Q[0-9]+" => None
			case other => this.mapSector(other)
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
			case "geo_coords" => this.normalizeCoords
			case "geo_country" => this.normalizeCountry
			case _ => identity
		}
	}
}
