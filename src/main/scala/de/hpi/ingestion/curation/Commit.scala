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
import play.api.libs.json._

class Commit extends SparkJob with JSONParser {
    appName = "Commit"
    configFile = "commit.xml"

    var subjects: RDD[Subject] = _
    var curatedSubjects: RDD[Subject] = _

    // $COVERAGE-OFF$
    /**
      * Loads a the Subjects from the Cassandra.
      * @param sc Spark Context used to load the RDDs
      */
    override def load(sc: SparkContext): Unit = {
        subjects = sc.cassandraTable[Subject](settings("keyspaceSubjectTable"), settings("subjectTable"))
    }

    /**
      * Saves the modified Subjects to the Cassandra.
      * @param sc Spark Context used to connect to the Cassandra or the HDFS
      */
    override def save(sc: SparkContext): Unit = {
        curatedSubjects.saveToCassandra(settings("keyspaceSubjectTable"), settings("subjectTable"))
    }
    // $COVERAGE-ON$

    /**
      * Parses the passed JSON and modifies the Subjects accordingly.
      * @param sc Spark Context used to e.g. broadcast variables
      */
    override def run(sc: SparkContext): Unit = {
        val commitJson = conf.commitJson
        val version = Version(appName, List("human"), sc, false, Option("subject"))

        val inputJson = Json.parse(commitJson)
        val createdEntries = extractMap(inputJson, List("created")).values
        val updatedEntries = extractMap(inputJson, List("updated")).values
        val deletedEntries = extractMap(inputJson, List("deleted")).values

        val createdSubjects = createdEntries.flatMap(createSubject(_, version))

        // ids of master Subjects that will be updated
        val updatedMasterIds = updatedEntries
            .flatMap(extractString(_, List("master")))
            .map(UUID.fromString(_))
            .toSet
        // the master Subjects that will be updated
        val updatedMasterSubjects = subjects.filter(subject => updatedMasterIds(subject.id))
        val updatedSubjects = updatedEntries
            .filter(subjectJson => extractString(subjectJson, List("master")).isDefined)
            .map { subjectJson =>
                val masterId = extractString(subjectJson, List("master"))
                    .map(UUID.fromString(_))
                    .get
                val masterSubject = updatedMasterSubjects.filter(_.id == masterId).first()
                updateSubject(masterSubject, subjectJson, version)
            }

        val deletedSubjects = deletedEntries
            .map(subJson => deleteSubject(UUID.fromString(extractString(subJson, List("master")).get), version))

        curatedSubjects = sc.parallelize(createdSubjects.toList ++ updatedSubjects.toList ++ deletedSubjects.toList)
    }

    /**
      * Splits the property values sent as String into a List of values.
      * @param property the property that is split
      * @param propertyValue JSON String of the values of the property
      * @return tuple of the property and the split values
      */
    def extractProperty(property: String, propertyValue: JsValue): (String, List[String]) = {
        val values = propertyValue.as[String].split(";")
        var splitValues = values.toList
        if(property == "geo_coords") {
            splitValues = values
                .grouped(2)
                .filter(_.length == 2)
                .map(_.mkString(";"))
                .toList
        }
        (property, splitValues)
    }

    /**
      * Extracts the Subject relations from the JSON serialization sent by the Curation Interface.
      * @param jsonData Map containing the target Subject and a Map containing the relations towards the target Subject
      * @return the extracted relations
      */
    def extractRelations(jsonData: Map[String, JsValue]): Map[UUID, Map[String, String]] = {
        jsonData.map { case (relationId, relationsJson) =>
            val destinationId = UUID.fromString(relationId)
            val relationValues = relationsJson.as[JsObject].fields.collect {
                case (key: String, value: JsString) => (key, value.as[String])
                case (key: String, JsNull) => (key, "")
            }.toMap
            (destinationId, relationValues)
        }
    }

    /**
      * Extracts the aliases from the respective field in the Commit JSON. Returns the extracted aliases or an empty
      * List if there were no aliases.
      * @param jsonData JSON object containing the alias field
      * @return List of aliases as Strings
      */
    def extractAliases(jsonData: JsValue): List[String] = {
        getValue(jsonData, List("aliases"))
            .collect { case aliases: JsArray => aliases.as[List[String]] }
            .toList
            .flatten
    }

    /**
      * Creates a Subject from the given JSON Object.
      * @param json JSON Object containing the data
      * @param version Version used for versioning
      * @return Subject extracted from the JSON Object
      */
    def createSubject(json: JsValue, version: Version): List[Subject] = {
        val masterId = extractString(json, List("id"))
            .map(UUID.fromString(_))
            .get
        val master = Subject.master(masterId)
        val subject = Subject.empty(datasource = "human")

        val sm = new SubjectManager(subject, version)
        sm.setName(extractString(json, List("name")))
        val aliases = extractAliases(json)
        sm.setAliases(aliases)
        sm.setCategory(extractString(json, List("category")))
        val properties = extractMap(json, List("properties")).map((extractProperty _).tupled)
        sm.setProperties(properties)

        val relations = extractRelations(extractMap(json, List("relations")))
        sm.setRelations(relations)
        sm.setMaster(master.id)
        List(subject, master)
    }

    /**
      * Creates a tombstone Subject for the given master UUID.
      * @param masterID UUID of the master node
      * @param version Version used for versioning
      * @return tombstone Subject
      */
    def deleteSubject(masterID: UUID, version: Version): Subject = {
        val deleteProperty = Map("deleted" -> List("true"))
        val subject = Subject.empty(datasource = "human")
        val sm = new SubjectManager(subject, version)
        sm.setMaster(masterID)
        sm.addProperties(deleteProperty)
        subject
    }

    /**
      * Updates a given Subject with the data of a JSON object.
      * @param masterSubject Subject to be updated
      * @param subjectJson JSON Object containing the updated data
      * @param version Version used for versioning
      * @return updated Subject
      */
    def updateSubject(masterSubject: Subject, subjectJson: JsValue, version: Version): Subject = {
        val subject = Subject.empty(datasource = "human")
        val sm = new SubjectManager(subject, version)

        // set name if it contains a different value
        extractString(subjectJson, List("name"))
            .filterNot(masterSubject.name.contains)
            .foreach(sm.setName(_, Map()))

        // set aliases if they contain different values
        val aliases = extractAliases(subjectJson)
        if (aliases != masterSubject.aliases) {
            sm.setAliases(aliases)
        }

        // set category if it contains a different value
        extractString(subjectJson, List("category"))
            .filterNot(masterSubject.category.contains)
            .foreach(sm.setCategory(_, Map()))

        // set properties if they contain different values
        val properties = extractMap(subjectJson, List("properties")).map((extractProperty _).tupled)
        val changedProperties = properties.toSet.diff(masterSubject.properties.toSet).toMap
        if (changedProperties.nonEmpty) {
            sm.addProperties(changedProperties)
        }

        // set relations if they contain different values
        val relations = extractRelations(extractMap(subjectJson, List("relations")))
        val changedRelations = relations.toSet.diff(masterSubject.relations.toSet).toMap
        if (changedRelations.nonEmpty) {
            sm.addRelations(changedRelations)
        }

        sm.setMaster(masterSubject.id)
        subject
    }
}
