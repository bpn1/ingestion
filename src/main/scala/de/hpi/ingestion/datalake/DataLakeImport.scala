package de.hpi.ingestion.datalake

import java.net.URL

import de.hpi.companies.algo.Tag
import de.hpi.companies.algo.classifier.AClassifier
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
	def filterEntities(entity: T): Boolean

	/**
	  * Translates an entity of the datasource into a Subject, which is the schema of the staging table.
	  *
	  * @param entity  the object to be converted to a Subject
	  * @param version the version of the new Subject
	  * @param mapping Map of the attribute renaming schema for the attribute normalization
	  * @return a new Subject created from the given object
	  */
	def translateToSubject(
		entity: T,
		version: Version,
		mapping: Map[String, List[String]],
		strategies: Map[String, List[String]],
		classifier: AClassifier[Tag]
	): Subject

	/**
	  * Parses the normalization config file into a Map.
	  *
	  * @param url the path to the config file
	  * @return a Map containing the normalized attributes mapped to the new subject attributes
	  */
	def parseNormalizationConfig(url: URL): Map[String, List[String]]

	/**
	  * Parses the normalization config file into a Map.
	  *
	  * @param path the path to the config file
	  * @return a Map containing the normalized attributes mapped to the new subject attributes
	  */
	def parseNormalizationConfig(path: String): Map[String, List[String]]

	/**
	  * Parses the category normalization config file into a Map.
	  *
	  * @param url the path to the config file
	  * @return a Map containing the normalized categories
	  */
	def parseCategoryConfig(url: URL): Map[String, List[String]]

	/**
	  * Parses the category normalization config file into a Map.
	  *
	  * @param path the path to the config file
	  * @return a Map containing the normalized categories
	  */
	def parseCategoryConfig(path: String): Map[String, List[String]]

	/**
	  * Normalizes a given entity into a map.
	  *
	  * @param entity the object to be normalized
	  * @return a Map containing the normalized information of the entity
	  */
	def normalizeProperties(
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
	def normalizeAttribute(
		attribute: String,
		values: List[String],
		strategies: Map[String, List[String]]
	): List[String]

	/**
	  * Extracts the legal form of a name
	  * @param name Name of the entity
	  * @return List of legal form occurrences in the name
	  */
	def extractLegalForm(name: String, classifier: AClassifier[Tag]): List[String]

	/**
	  * Getter for the company split classifier
	  * @return The classifier
	  */
	def classifier: AClassifier[Tag]
}
