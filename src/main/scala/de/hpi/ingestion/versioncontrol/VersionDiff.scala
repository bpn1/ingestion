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

package de.hpi.ingestion.versioncontrol

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import java.util.UUID
import play.api.libs.json._
import scala.collection.mutable
import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.TupleImplicits._
import de.hpi.ingestion.versioncontrol.models.{HistoryEntry, SubjectDiff}

/**
  * Compares two versions of the Subject table given their TimeUUIDs as command line arguments and writes the
  * differences of each Subject as JSON file to the HDFS.
  */
class VersionDiff extends SparkJob {
	import VersionDiff._
	appName = "VersionDiff"
	val keyspace = "datalake"
	val tablename = "subject"
	val outputTablename = "versiondiff"

	var subjects: RDD[Subject] = _
	var subjectDiff: RDD[SubjectDiff] = _

	// $COVERAGE-OFF$
	/**
	  * Loads subjects from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext): Unit = {
		subjects = sc.cassandraTable[Subject](keyspace, tablename)
	}

	/**
	  * Writes the JSON diffs to Cassandra.
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		subjectDiff.saveToCassandra(keyspace, outputTablename)
	}
	// $COVERAGE-ON$

	/**
	  * Creates a diff for each subject containing the deletions and additions of every field between the two versions.
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @return List of RDDs containing the output data
	  */
	override def run(sc: SparkContext): Unit = {
		val (oldVersion, newVersion) = conf.diffVersions.map(UUID.fromString) match {
			case List(version1, version2) => versionOrder(version1, version2)
		}
		subjectDiff = subjects
			.map(retrieveVersions(_, oldVersion, newVersion))
			.map(historyToDiff(_, oldVersion, newVersion))
			.filter(_.hasChanges())
	}

	/**
	  * Asserts that two versions are given as program arguments.
	  * @return true if there are at least two arguments provided
	  */
	override def assertConditions(): Boolean = conf.diffVersionsOpt.isDefined
}

object VersionDiff {
	val NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L
	/**
	  * Extracts the time component of a TimeUUID.
	  * Source: https://git.io/vSxjU
	  * @param uuid TimeUUID used to extract the time
	  * @return time as long
	  */
	def timeFromUUID(uuid: UUID): Long = (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000

	/**
	  * Returns list of values of the given version or a previous version that changed the given field.
	  * @param queryVersion TimeUUID of the wanted version of the attributes
	  * @param versionList list of all existing versions of the field
	  * @return value of the field at the point of the queried version
	  */
	def findValues(queryVersion: UUID, versionList: List[Version]): List[String] = {
		val queryValue = versionList.find(_.version == queryVersion).map(_.value)
		val olderVersions = versionList.filter(v => timeFromUUID(v.version) < timeFromUUID(queryVersion))
		val olderValue = if(olderVersions.nonEmpty) {
			val lastVersion = olderVersions.maxBy(v => timeFromUUID(v.version))
			Option(lastVersion.value)
		} else {
			None
		}
		queryValue.orElse(olderValue).getOrElse(Nil)
	}

	/**
	  * Returns tuple of old and new values for the given field.
	  * @param oldVersion TimeUUID of the older version
	  * @param newVersion TimeUUID of the newer version
	  * @param versionList list of all existing versions of the field
	  * @return Option of tuple of old and new values
	  */
	def createValueList(
		oldVersion: UUID,
		newVersion: UUID,
		versionList: List[Version]
	): Option[(List[String], List[String])] = {
		val oldValues = findValues(oldVersion, versionList)
		val newValues = findValues(newVersion, versionList)
		(oldValues, newValues) match {
			case (Nil, Nil) => None
			case (x, y) if oldVersion == newVersion => Option((Nil, y))
			case x => Option(x)
		}
	}

	/**
	  * Returns a JSON object containing the removals and additions.
	  * @param valueLists Tuple of old and new values of the field
	  * @return Option of the JSON object with "-" and "+" field. Returns None if valueLists None or there
	  *         are no changes in the data.
	  */
	def diffLists(valueLists: Option[(List[String], List[String])]): Option[JsValue] = {
		valueLists.map { case (oldValues, newValues) =>
			val removals = oldValues.filterNot(newValues.toSet)
			val additions = newValues.filterNot(oldValues.toSet)
			val jsonObject = mutable.Map[String, JsValue]()
			if(removals.nonEmpty) jsonObject("-") = Json.toJson(removals)
			if(additions.nonEmpty) jsonObject("+") = Json.toJson(additions)
			jsonObject.toMap
		}.filter(_.nonEmpty)
			.map(Json.toJson(_))
	}

	/**
	  * Transforms version histories to tuples of old and new values of every field.
	  * @param subject Subject containing the data
	  * @param oldVersion TimeUUID of the older version
	  * @param newVersion TimeUUID of the newer version
	  * @return History Entry containing the diffs of each field of the Subject
	  */
	def retrieveVersions(
		subject: Subject,
		oldVersion: UUID,
		newVersion: UUID
	): HistoryEntry = {
		val nameList = createValueList(oldVersion, newVersion, subject.name_history)
		val masterList = createValueList(oldVersion, newVersion, subject.master_history)
		val aliasesList = createValueList(oldVersion, newVersion, subject.aliases_history)
		val categoryList = createValueList(oldVersion, newVersion, subject.category_history)
		val properties = subject.properties_history.mapValues(createValueList(oldVersion, newVersion, _))
		val relations = subject.relations_history.mapValues(_.mapValues(createValueList(oldVersion, newVersion, _)))
		HistoryEntry(subject.id, nameList, masterList, aliasesList, categoryList, properties, relations)
	}

	/**
	  * Creates a diff of the values in the History Entry and writes it to a SubjectDiff entry
	  * @param entry History Entry containing the data of two versions
	  * @return SubjectDiff case class containing the calculated differences
	  */
	def historyToDiff(entry: HistoryEntry, oldVersion: UUID, newVersion: UUID): SubjectDiff = {
		val subjectDiff = SubjectDiff(oldVersion, newVersion, entry.id)
		subjectDiff.name = diffLists(entry.name).map(_.toString)
		subjectDiff.master = diffLists(entry.master).map(_.toString)
		subjectDiff.aliases = diffLists(entry.aliases).map(_.toString)
		subjectDiff.category = diffLists(entry.category).map(_.toString)
		val propertyDiff = entry.properties.mapValues(diffLists).filter(_._2.nonEmpty)
		if(propertyDiff.nonEmpty) subjectDiff.properties = Option(Json.toJson(propertyDiff).toString)
		val relationsMap = entry.relations
			.mapValues(_.mapValues(diffLists))
			.map(_.map(_.toString, _.filter(_._2.nonEmpty).mapValues(_.get)))
			.filter(_._2.nonEmpty)
			.mapValues(Json.toJson(_))
		if(relationsMap.nonEmpty) subjectDiff.relations = Option(Json.toJson(relationsMap).toString)

		subjectDiff
	}

	/**
	  * Returns tuple of versions with the older version as first element.
	  * @param version1 first version
	  * @param version2 second version
	  * @return tuple of versions with the older version as first element
	  */
	def versionOrder(version1: UUID, version2: UUID): (UUID, UUID) = {
		if(timeFromUUID(version1) > timeFromUUID(version2)) {
			(version2, version1)
		} else {
			(version1, version2)
		}
	}
}
