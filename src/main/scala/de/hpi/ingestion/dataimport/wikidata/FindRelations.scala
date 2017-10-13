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

package de.hpi.ingestion.dataimport.wikidata

import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import scala.util.matching.Regex
import java.util.UUID

/**
  * Finds Wikidata relations between `Subjects`, translates the Wikidata ID relations intoa `Subject` UUID relations and
  * replaces the Wikidata IDs with their names.
  */
class FindRelations extends SparkJob {
	appName = "Find Relations v1.1"
	configFile = "wikidata_import.xml"
	val datasources = List("wikidata_20161117", "wikidata")

	var subjects: RDD[Subject] = _
	var subjectsWithRelations: RDD[Subject] = _

	// $COVERAGE-OFF$
	/**
	  * Loads the Subjects from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		subjects = sc.cassandraTable[Subject](settings("subjectKeyspace"), settings("subjectTable"))
	}

	/**
	  * Saves the Subjects with the new relations to the Cassandra.
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		subjectsWithRelations.saveToCassandra(settings("subjectKeyspace"), settings("subjectTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Finds relations of a Subject to Wikidata ids, resolves the name of the ids and adds a
	  * relation to the subject with the corresponding Wikidata id.
	  * @param subject Subject to find the relations for
	  * @param nameResolveMap map used to resolve Wikidata ids into names and uuids
	  * @param version Version to use for versioning in the Subject Manager
	  * @return Subject with added relations and resolved Wikidata ids
	  */
	def findRelations(
		subject: Subject,
		nameResolveMap: Map[String, (UUID, String)],
		version: Version
	): Subject = {
		val sm = new SubjectManager(subject, version)
		val idRegex = new Regex("^Q[0-9]+$")
		var relationsMap = Map[UUID, Map[String, String]]()
		val resolvedProperties = subject.properties
		    .filterKeys(_ != settings("wikidataIdKey"))
			.map { case (property, propertyValues) =>
				val (resolvableValues, doneValues) = propertyValues.partition(value =>
					idRegex.findFirstIn(value).isDefined && nameResolveMap.contains(value))

				// add relations to subject with the corresponding wikidata id
				relationsMap ++= resolvableValues
					.map(value => (nameResolveMap(value)._1, Map(property -> "")))
					.toMap[UUID, Map[String, String]]
				val resolvedValues = resolvableValues
				    .map { id =>
						nameResolveMap.get(id)
						    .map(_._2)
					    	.filter(_.nonEmpty)
							.getOrElse(id)
					}
				(property, resolvedValues ++ doneValues)
			}
		sm.overwriteProperties(resolvedProperties)
		sm.addRelations(relationsMap)
		subject
	}

	/**
	  * Creates map containing the wikidata id as key and the corresponding subjects id and name
	  * as value.
	  * @param subjects RDD of subjects
	  * @return map of wikidata ids mapping to the corresponding subject id and name
	  */
	def resolvableNamesMap(subjects: RDD[Subject]): Map[String, (UUID, String)] = {
		subjects
			.filter(_.properties.get(settings("wikidataIdKey")).exists(_.nonEmpty))
			.map { subject =>
				val wikidataId = subject.properties(settings("wikidataIdKey")).head
				val name = subject.name.orElse(subject.aliases.headOption).getOrElse(wikidataId)
				(wikidataId, (subject.id, name))
			}.collect
			.toMap
	}

	/**
	  * Transforms the Wikidata relations to Subject relations.
	  * @param sc Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		val nameResolveMap = resolvableNamesMap(subjects)
		val version = Version(appName, datasources, sc, false, settings.get("subjectTable"))
		subjectsWithRelations = subjects.map(findRelations(_, nameResolveMap, version))
	}
}
