/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	def extractLegalForm(name: String, classifier: AClassifier[Tag]): Option[String]

	/**
	  * Getter for the company split classifier
	  * @return The classifier
	  */
	def classifier: AClassifier[Tag]
}
