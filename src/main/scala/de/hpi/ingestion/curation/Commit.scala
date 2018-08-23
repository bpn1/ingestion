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
import de.hpi.ingestion.datamerge.Merging
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import play.api.libs.json._

class Commit extends SparkJob with JSONParser {
    appName = "Commit"
    configFile = "commit.xml"

    val humanDatasource = "human"

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
        val version = Version(appName, List(humanDatasource), sc, false, Option("subject"))
        val inputJson = Json.parse(commitJson)
        val createdAndDeletesSubjects = sc.parallelize(createSubjects(inputJson, version) ++
            deleteSubjects(inputJson, version))
        curatedSubjects = createdAndDeletesSubjects.union(updateSubjects(inputJson, version))
    }

    /**
      * Creates Subjects with the data in the Commit JSON passed to the Spark Job.
      * @param json the Commit JSON object containing all changes
      * @param version version used for Versioning of Subjects
      * @return List of created Subjects and their master Subjects
      */
    def createSubjects(json: JsValue, version: Version): List[Subject] = {
        val creationEntries = extractMap(json, List("created")).values
        creationEntries
            .flatMap(createSubject(_, version))
            .toList
    }

    /**
      * Updates Subjects with the data in the Commit JSON passed to the Spark Job.
      * @param json the Commit JSON object containing all changes
      * @param version version used for Versioning of Subjects
      * @return RDD of human Subjects containing the updates
      */
    def updateSubjects(json: JsValue, version: Version): RDD[Subject] = {
        val updateEntries = extractMap(json, List("updated")).values
        val masterUpdateEntries = updateEntries.filter(subJson =>
            extractString(subJson, List("master")) == extractString(subJson, List("id")))

        // ids of master Subjects that will be updated
        val updateMasterIds = masterUpdateEntries
            .flatMap(extractString(_, List("master")))
            .map(UUID.fromString(_))
            .toSet
        // the master Subjects that will be updated and their slaves
        val updateMasterAndSlaves = subjects
            .filter(subject => updateMasterIds(subject.master))
            .map(subject => (subject.master, List(subject)))
            .reduceByKey(_ ++ _)
        // already existing human Subjects for the updated master Subjects
        val duplicateHumanSubjects = subjects
            .filter(subject => updateMasterIds(subject.master) && subject.datasource == humanDatasource)
            .map(subject => (subject.master, subject))
            .collect
            .toMap
        // Map of the master ids and their update JSON
        val updateEntryMap = masterUpdateEntries
            .filter(subjectJson => extractString(subjectJson, List("master")).isDefined)
            .map { subjectJson =>
                val masterId = extractString(subjectJson, List("master"))
                    .map(UUID.fromString(_))
                    .get
                (masterId, subjectJson)
            }.toMap

        // the master Subjects and their slaves to which relations point that are updated
        val relationUpdateTargetIds = masterUpdateEntries
            .map(extractMap(_, List("relations")))
            .map(extractRelations)
            .flatMap(_.keySet)
            .toSet
        val relationUpdateTargets = subjects
            .filter(subject => relationUpdateTargetIds(subject.master) && subject.isSlave)
            .map(subject => (subject.master, Set(subject.id)))
            .reduceByKey(_ ++ _)
            .collect
            .toMap
        updateMasterAndSlaves.map { case (masterId, masterAndSlaves) =>
            val subjectJson = updateEntryMap(masterId)
            val masterSubject = masterAndSlaves.find(_.isMaster).get
            val slaves = masterAndSlaves.filter(_.isSlave)
            updateSubject(masterSubject, slaves, subjectJson, duplicateHumanSubjects, relationUpdateTargets, version)
        }
    }

    /**
      * Deletes the Subjects specified in the Commit JSON passed to the Spark Job.
      * @param json the Commit JSON object containing all changes
      * @param version version used for Versioning of Subjects
      * @return List of human Subjects containing the deleted flag
      */
    def deleteSubjects(json: JsValue, version: Version): List[Subject] = {
        val deletedEntries = extractMap(json, List("deleted")).values
        deletedEntries
            .filter(subJson => extractString(subJson, List("master")) == extractString(subJson, List("id")))
            .map(subJson => deleteSubject(UUID.fromString(extractString(subJson, List("master")).get), version))
            .toList
    }

    /**
      * Transforms the passed property values into a List of values. If the input is a JSON String, it is split with a
      * separator. If it is a JSON Array, it is simply cast to a List
      * @param property name of the property that is split
      * @param propertyValue JSON value containing the values of the property
      * @return tuple of the property and the extracted values as a List of Strings
      */
    def extractProperty(property: String, propertyValue: JsValue): (String, List[String]) = {
        val values = propertyValue match {
            case x: JsString => x.as[String].split("; ").toList
            case x: JsArray => x.as[List[String]]
            case _ => Nil
        }
        (property, values)
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
        val subject = Subject.empty(datasource = humanDatasource)

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
        val deleteProperty = Map(Subject.deletePropertyKey -> Subject.deletePropertyValue)
        val subject = Subject.empty(datasource = humanDatasource)
        val sm = new SubjectManager(subject, version)
        sm.setMaster(masterID)
        sm.addProperties(deleteProperty)
        subject
    }

    /**
      * Updates a given Subject with the data of a JSON object.
      * @param masterSubject Subject to be updated
      * @param slaves slave Subjects of the updated master Subject
      * @param subjectJson JSON Object containing the updated data
      * @param humanSubjects Map of already existing human Subjects
      * @param relationUpdateTargets Map of all master UUIDs and their slaves to which relations point that are updated
      * @param version Version used for versioning
      * @return updated Subject
      */
    def updateSubject(
        masterSubject: Subject,
        slaves: List[Subject] = Nil,
        subjectJson: JsValue,
        humanSubjects: Map[UUID, Subject] = Map(),
        relationUpdateTargets: Map[UUID, Set[UUID]] = Map(),
        version: Version
    ): Subject = {
        val subject = humanSubjects.getOrElse(masterSubject.id, Subject.empty(datasource = humanDatasource))
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
            sm.setProperties(changedProperties)
        }

        // set relations if they contain different values
        val relations = extractRelations(extractMap(subjectJson, List("relations")))
        val changedRelations = relations.toSet.diff(masterSubject.relations.toSet).toMap
        if (changedRelations.nonEmpty) {
            val redirectedChangedRelations = redirectRelations(changedRelations, slaves, relationUpdateTargets)
            sm.setRelations(redirectedChangedRelations)
        }

        sm.setMaster(masterSubject.id)
        subject
    }

    /**
      * Changed relations in the Curation Interface point to master Subjects. These are redirected to point to slave
      * Subjects instead. The input relations should not contain any master or slave relations but only "actual"
      * relations. Newly added relations will be redirected to one of the slave Subjects of the master Subject the
      * relation points to.
      * @param updateRelations the relations that will be redirected
      * @param slaves List of slave Subjects of the master Subject that is updated. These will be used to find the
      * correct target slave Subject for the redirected relations.
      * @param relationUpdateTargets Map containing master Subject ids pointing to all their slave Subject ids. Only
      * contains these pairs for Subjects, to which relations point that are updated.
      * @return the redirected relations. Any relation that was not able to be redirected is excluded
      */
    def redirectRelations(
        updateRelations: Map[UUID, Map[String, String]],
        slaves: List[Subject],
        relationUpdateTargets: Map[UUID, Set[UUID]]
    ): Map[UUID, Map[String, String]] = {
        val sortedSlaves = Merging.sourcePriority.flatMap(source => slaves.find(_.datasource == source))
        val redirectedRelations = updateRelations.toList.flatMap { case (id, relationMap) =>
            val targetSlaves = relationUpdateTargets.getOrElse(id, Set())
            relationMap
                .toList
                .flatMap { case (key, value) =>
                    val slaveId = sortedSlaves
                        .flatMap { slave =>
                            slave.findRelationDestination(targetSlaves, key, value)
                        }.headOption
                    // if the current relation is newly added, the destination slave node is the first slave in the set
                    val targetSlave = slaveId.orElse(targetSlaves.headOption)
                    targetSlave.map(targetId => (targetId, List((key, value))))
                }
        }
        redirectedRelations
            .groupBy(_._1)
            .map { case (id, groupedRelations) =>
                val relations = groupedRelations
                    .map(_._2)
                    .reduce(_ ++ _)
                    .toMap
                (id, relations)
            }
    }
}
