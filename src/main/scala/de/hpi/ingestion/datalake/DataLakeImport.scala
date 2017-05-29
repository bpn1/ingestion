package de.hpi.ingestion.datalake

import java.net.URL

import de.hpi.ingestion.datalake.models.{DLImportEntity, Subject, Version}

/**
  * Trait for imports into the Datalake.
  * @tparam T Entity type that will be translated into Subjects.
  */
trait DataLakeImport[T <: DLImportEntity] extends Serializable {
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
	protected def translateToSubject(
		entity: T,
		version: Version,
		mapping: Map[String, List[String]],
		strategies: Map[String, List[String]]
	): Subject

	/**
	  * Parses the normalization config file into a Map.
	  *
	  * @param url the path to the config file
	  * @return a Map containing the normalized attributes mapped to the new subject attributes
	  */
	protected def parseNormalizationConfig(url: URL): Map[String, List[String]]

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
		mapping: Map[String, List[String]],
		strategies: Map[String, List[String]]
	): Map[String, List[String]]

	/**
	  * Normalizes a given attribute
	  * @param attribute attribute name
	  * @param values attribute values
	  * @return normalized attribute values
	  */
	protected def normalizeAttribute(
		attribute: String,
		values: List[String],
		strategies: Map[String, List[String]]
	): List[String]
}

import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.dataimport.dbpedia.models.DBpediaEntity
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.dataimport.dbpedia.DBpediaDataLakeImport
