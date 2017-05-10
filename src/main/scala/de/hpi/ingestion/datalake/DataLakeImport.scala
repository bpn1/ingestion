package de.hpi.ingestion.datalake

import java.net.URL
import de.hpi.ingestion.datalake.models.{DLImportEntity, Subject, Version, WZCategory}
import scala.collection.mutable
import scala.io.Source
import scala.xml.XML

/**
  * Trait for imports into the Datalake
  * @tparam T
  */
trait DataLakeImport[T <: DLImportEntity] extends Serializable {
	val settings = mutable.Map[String, String]()
	val outputKeyspace = "datalake"
	val outputTable = "subject_temp"
	val versionTable = "version"

	/**
	  * Filters non companies.
	  *
	  * @param entity the entity to be filtered
	  * @return Boolean if the entity matches the filter
	  */
	protected def filterEntities(entity: T): Boolean

	/**
	  * Translates an entity of the datasource into a Subject, which is the schema of the staging table.
	  *
	  * @param entity  the object to be converted to a Subject
	  * @param version the version of the new Subject
	  * @param mapping Map of the attribute renaming schema for the attribute normalization
	  * @return a new Subject created from the given object
	  */
	protected def translateToSubject(entity: T, version: Version, mapping: Map[String, List[String]]): Subject

	/**
	  * Parses the normalization config file into a Map.
	  *
	  * @param url the path to the config file
	  * @return a Map containing the normalized attributes mapped to the new subject attributes
	  */
	protected def parseNormalizationConfig(url: URL): Map[String, List[String]]

	protected def parseCategorizationConfig(url: URL): Map[WZCategory, List[WZCategory]] = {
		val xml = XML.loadString(Source
			.fromURL(url)
			.getLines()
			.mkString("\n"))

		val categoryMap = mutable.Map[WZCategory, List[WZCategory]]()
		(xml \\ "Workbook" \ "Worksheet" \ "Table" \ "Row").foreach { row =>
			val cells = (row \ "Cell").toList
			val label = (cells(1) \ "Data").text
			val description = (cells(2) \ "Data").text
			val category = WZCategory(label, description)

			if(label == "A") {
				categoryMap(category) = Nil
			} else {
				val key = categoryMap.last._1
				categoryMap(key) = categoryMap.getOrElse(key, Nil) ::: List(category)
			}
		}
		categoryMap.toMap
	}

	protected def parseCategorizationConfig(path: String): Map[WZCategory, List[WZCategory]] = {
		val url = getClass.getResource(s"/$path")
		this.parseCategorizationConfig(url)
	}

	/**
	  * Parses the normalization config file into a Map.
	  *
	  * @param path the path to the config file
	  * @return a Map containing the normalized attributes mapped to the new subject attributes
	  */
	protected def parseNormalizationConfig(path: String): Map[String, List[String]]

	/**
	  * Normalizes a given entity into a map.
	  *
	  * @param entity the object to be normalized
	  * @return a Map containing the normalized information of the entity
	  */
	protected def normalizeProperties(
		entity: T,
		mapping: Map[String, List[String]]
	): Map[String, List[String]]

	/**
	  * Normalizes a given attribute
	  * @param attribute attribute name
	  * @param values attribute values
	  * @return normalized attribute values
	  */
	protected def normalizeAttribute(attribute: String, values: List[String]): List[String]
}
