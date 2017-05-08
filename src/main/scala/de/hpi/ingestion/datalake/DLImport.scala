package de.hpi.ingestion.datalake

import java.net.URL

import de.hpi.ingestion.datalake.models.{DLImportEntity, Subject, Version}

import scala.collection.mutable

trait DLImport[T <: DLImportEntity] extends Serializable {
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
/*
	protected def normalizeValues(
		property: (String, List[String])
	): Subject
	*/
}
