package de.hpi.ingestion.curation

import java.util.UUID

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.JSONParser
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import play.api.libs.json.{JsArray, JsValue, Json}

object Commit extends SparkJob with JSONParser[Boolean] {
	appName = "Commit"
	configFile = ""

	/**
		* Loads a number of input RDDs used in the job.
		*
		* @param sc   Spark Context used to load the RDDs
		* @param args arguments of the program
		* @return List of RDDs containing the data processed in the job.
		*/
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = sc.cassandraTable[Subject]("datalake", "subject")
		List(subjects).toAnyRDD()
	}

	/**
		* Executes the data processing of this job and produces the output data.
		*
		* @param input List of RDDs containing the input data
		* @param sc    Spark Context used to e.g. broadcast variables
		* @param args  arguments of the program
		* @return List of RDDs containing the output data
		*/
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		// TODO: parsing from args removes double quotes which are needed for parsing
		val commitJson = args(0)
		val subjects = input.take(1).fromAnyRDD[Subject]().head
		val templateVersion = Version(appName, List("human"), sc, false, Option("subject"))

		val inputJson = Json.parse(commitJson)
		val created = extractMap(inputJson, List("created")).values
		val updated = extractMap(inputJson, List("updated")).values
		val deleted = extractMap(inputJson, List("deleted")).values

		val deletedSubjects = deleted
			.map(subJson => deleteSubject(UUID.fromString(extractString(subJson, List("master")).get), templateVersion))

		val createdSubjects = created.map(subJson => createSubject(subJson, templateVersion))

		val updatedMasterIds: List[UUID] =
			updated.map(subJson => UUID.fromString(extractString(subJson, List("master")).get)).toList

		val updatedMasterSubjects =
			subjects.filter(subject => updatedMasterIds.contains(subject.master) && subject.datasource == "dbpedia")

		val updatedSubjects = updated
			.map(sub => (updatedMasterSubjects.filter(subject =>
				subject.master == UUID.fromString(extractString(sub, List("master")).get)).first, sub))
		  	.map{case (oldSubject: Subject, json: JsValue) => updateSubject(oldSubject, json, templateVersion)}

		val humanSubjects: RDD[Subject] =
			sc.parallelize(createdSubjects.toList ++ updatedSubjects.toList ++ deletedSubjects.toList)

		List(humanSubjects).toAnyRDD()
	}

	/**
		* Saves the output data to e.g. the Cassandra or the HDFS.
		*
		* @param output List of RDDs containing the output of the job
		* @param sc     Spark Context used to connect to the Cassandra or the HDFS
		* @param args   arguments of the program
		*/
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
	    	.fromAnyRDD[Subject]()
			.head
			.saveToCassandra("datalake", "subject")
	}

	/**
		* Extracts the JSON data from a JSON object to an Entity.
		*
		* @param json JSON-Object containing the data
		* @return Entity containing the parsed data
		*/
	override def fillEntityValues(json: JsValue): Boolean = {
		true
	}

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
		sm.setProperties(extractMap(json, List("properties")).map{case (key, value) => (key, value.as[List[String]])})

		val relations: Map[UUID, Map[String, String]] = extractMap(json, List("realtions"))
			.map{ case (relationId, relationsJson) =>
				UUID.fromString(relationId) -> relationsJson.as[Map[String, String]]
			}

		sm.setRelations(relations)
		subject
	}


	def deleteSubject(masterID: UUID, templateVersion: Version): Subject = {
		val deleteProperty = Map("deleted" -> List("true"))

		val subject = Subject.empty(datasource = "human")
		val sm = new SubjectManager(subject, templateVersion)
		sm.setMaster(masterID)
		sm.setProperties(deleteProperty)
		subject
	}

	def updateSubject(oldSubject: Subject, json: JsValue, templateVersion: Version): Subject = {
		val id = UUID.fromString(extractString(json, List("id")).get)
		val subject = Subject.empty(id = id, datasource = "human")
		val sm = new SubjectManager(subject, templateVersion)

		val name: Option[String] = extractString(json, List("name"))
		val aliases: List[String] = getValue(json, List("aliases")).get match {
			case arr: JsArray => arr.as[List[String]]
			case _  => Nil
		}
		val category: Option[String] = extractString(json, List("category"))
		val properties: Map[String, List[String]] =
			extractMap(json, List("properties")).map{case (key, value) => (key, value.as[List[String]])}
		val relations: Map[UUID, Map[String, String]] = extractMap(json, List("realtions"))
			.map{ case (relationId, relationsJson) =>
				UUID.fromString(relationId) -> relationsJson.as[Map[String, String]]
			}.toMap[UUID, Map[String, String]]

		sm.setMaster(oldSubject.master)

		if (name != oldSubject.name) {
			sm.setName(name)
		}

		if (category != oldSubject.category) {
			sm.setCategory(category)
		}

		if (properties != oldSubject.properties) {
			val diffProperties = (properties.toSet diff oldSubject.properties.toSet).toMap
			// all changed properties are set and overwrites human properties
			sm.overwriteProperties(diffProperties)
		}

		if (aliases != oldSubject.aliases) {
			val diffAliases = aliases diff oldSubject.aliases
			sm.setAliases(diffAliases)
		}

		if (relations != oldSubject.relations) {
			val diffRelations = (relations.toSet diff oldSubject.relations.toSet).toMap
			sm.setRelations(diffRelations)
		}

		subject
	}


}
