package de.hpi.ingestion.dataimport

import scala.io.Source
import scala.xml.XML

class NormalizationStrategy(categoryConfigFile: String) {
	var mapping = Map[String, List[String]]()

	/**
	  * Parses the categorization file
	  * @return Map containing the categorization mapping
	  */
	protected def parseNormalizationConfig(): Map[String, List[String]] = {
		val url = this.getClass.getResource(s"/${this.categoryConfigFile}")
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
	protected def mapSector(sector: String): List[String] = this.mapping.getOrElse(sector, List(sector))
}
