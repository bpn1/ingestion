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

package de.hpi.ingestion.curation

import java.util.UUID
import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.JSONParser
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import play.api.libs.json.{JsArray, JsValue, Json}

class Commit extends SparkJob with JSONParser[Boolean] {
	appName = "Commit"
	configFile = "commit.xml"

	var subjects: RDD[Subject] = _
	var curatedSubjects: RDD[Subject] = _

	// $COVERAGE-OFF$
	/**
	  * Loads a number of input RDDs used in the job.
	  * @param sc   Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		subjects = sc.cassandraTable[Subject](settings("keyspaceSubjectTable"), settings("subjectTable"))
	}

	/**
	  * Saves the output data to e.g. the Cassandra or the HDFS.
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		curatedSubjects.saveToCassandra(settings("keyspaceSubjectTable"), settings("subjectTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Executes the data processing of this job and produces the output data.
	  * @param sc    Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		val commitJson = conf.commitJson
		val templateVersion = Version(appName, List("human"), sc, false, Option("subject"))

		val inputJson = Json.parse(commitJson)
		val createdEntries = extractMap(inputJson, List("created")).values
		val updatedEntries = extractMap(inputJson, List("updated")).values
		val deletedEntries = extractMap(inputJson, List("deleted")).values

		val deletedSubjects = deletedEntries
			.map(subJson => deleteSubject(UUID.fromString(extractString(subJson, List("master")).get), templateVersion))
		val createdSubjects = createdEntries.map(createSubject(_, templateVersion))

		val updatedMasterIds = updatedEntries
			.map(subJson => UUID.fromString(extractString(subJson, List("master")).get))
			.toList

		val updatedMasterSubjects = subjects
			.filter(subject => updatedMasterIds.contains(subject.master) && subject.datasource == "master")

		val updatedSubjects = updatedEntries
			.map { subjectJson =>
				val oldSubject = updatedMasterSubjects
					.filter(_.master == UUID.fromString(extractString(subjectJson, List("master")).get))
					.first
				(oldSubject, subjectJson)
			}.map { case (oldSubject, json) => updateSubject(oldSubject, json, templateVersion) }
		curatedSubjects = sc.parallelize(createdSubjects.toList ++ updatedSubjects.toList ++ deletedSubjects.toList)
	}

	/**
	  * Dummy method since it needs to be overwritten to extend the JSONParser trait.
	  *
	  * @param json JSON-Object containing the data
	  * @return Entity containing the parsed data
	  */
	override def fillEntityValues(json: JsValue): Boolean = true

	/**
	  * Creates a Subject from the given JSON Object.
	  * @param json JSON Object containing the data
	  * @param templateVersion Version used for versioning
	  * @return Subject extracted from the JSON Object
	  */
	def createSubject(json: JsValue, templateVersion: Version): Subject = {
		val id = UUID.fromString(extractString(json, List("id")).get)
		val subject = Subject.empty(id = id, datasource = "human")

		val sm = new SubjectManager(subject, templateVersion)
		sm.setMaster(UUID.fromString(extractString(json, List("master")).get))
		sm.setName(extractString(json, List("name")))
		sm.setAliases(getValue(json, List("aliases")).get match {
			case arr: JsArray => arr.as[List[String]]
			case _  => Nil
		})
		sm.setCategory(extractString(json, List("category")))
		sm.setProperties(extractMap(json, List("properties")).mapValues(_.as[List[String]]).map(identity))

		val relations = extractMap(json, List("relations"))
			.map { case (relationId, relationsJson) =>
				UUID.fromString(relationId) -> relationsJson.as[Map[String, String]]
			 // TODO handle case when score is null, fails otherwise
			}
		sm.setRelations(relations)
		subject
	}

	/**
	  * Creates a tombstone Subject for the given master UUID.
	  * @param masterID UUID of the master node
	  * @param templateVersion Version used for versioning
	  * @return tombstone Subject
	  */
	def deleteSubject(masterID: UUID, templateVersion: Version): Subject = {
		val deleteProperty = Map("deleted" -> List("true"))
		val subject = Subject.empty(datasource = "human")
		val sm = new SubjectManager(subject, templateVersion)
		sm.setMaster(masterID)
		sm.setProperties(deleteProperty)
		subject
	}

	/**
	  * Updates a given Subject with the data of a JSON object.
	  * @param oldSubject Subject to be updated
	  * @param json JSON Object containing the updated data
	  * @param templateVersion Version used for versioning
	  * @return updated Subject
	  */
	def updateSubject(oldSubject: Subject, json: JsValue, templateVersion: Version): Subject = {
		val id = UUID.fromString(extractString(json, List("id")).get)
		val subject = Subject.empty(id = id, datasource = "human")
		val sm = new SubjectManager(subject, templateVersion)

		val name = extractString(json, List("name"))
		val aliases = getValue(json, List("aliases")).get match {
			case arr: JsArray => arr.as[List[String]]
			case _  => Nil
		}
		val category = extractString(json, List("category"))
		val properties = extractMap(json, List("properties")).mapValues(_.as[List[String]]).map(identity)
		val relations = extractMap(json, List("relations"))
			.map { case (relationId, relationsJson) =>
				UUID.fromString(relationId) -> relationsJson.as[Map[String, String]]
				// TODO handle case when score is null, fails otherwise
			}
		sm.setMaster(oldSubject.master)
		if (name != oldSubject.name) {
			sm.setName(name)
		}
		if (category != oldSubject.category) {
			sm.setCategory(category)
		}
		if (properties != oldSubject.properties) {
			val diffProperties = properties.toSet.diff(oldSubject.properties.toSet).toMap
			// all changed properties are set and overwrites human properties
			sm.overwriteProperties(diffProperties)
		}
		if (aliases != oldSubject.aliases) {
			val diffAliases = aliases diff oldSubject.aliases
			sm.setAliases(diffAliases)
		}
		if (relations != oldSubject.relations) {
			val diffRelations = relations.toSet.diff(oldSubject.relations.toSet).toMap
			sm.setRelations(diffRelations)
		}
		subject
	}
}
