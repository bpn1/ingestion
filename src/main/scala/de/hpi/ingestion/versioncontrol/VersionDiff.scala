package de.hpi.ingestion.versioncontrol

import de.hpi.ingestion.datalake.models._
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import java.util.UUID

import de.hpi.ingestion.framework.SparkJob
import play.api.libs.json._

import scala.collection.mutable
import de.hpi.ingestion.implicits.TupleImplicits._
import de.hpi.ingestion.versioncontrol.models.HistoryEntry
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._

/**
  * Compares two versions of the Subject table given their TimeUUIDs as command line arguments and writes the
  * differences of each Subject as JSON file to the HDFS.
  */
object VersionDiff extends SparkJob {
	appName = "VersionDiff"
	val keyspace = "datalake"
	val tablename = "subject"
	val NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L

	// $COVERAGE-OFF$
	/**
	  * Loads subjects from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = sc.cassandraTable[Subject](keyspace, tablename)
		List(subjects.asInstanceOf[RDD[Any]])
	}

	/**
	  * Writes the JSON diff to the HDFS.
	  * @param output first element is the RDD of JSON diffs
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[JsValue]()
			.head
			.saveAsTextFile("versionDiff_" + System.currentTimeMillis / 1000)
	}
	// $COVERAGE-ON$

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
			case t => Option(t)
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
		val aliasesList = createValueList(oldVersion, newVersion, subject.aliases_history)
		val categoryList = createValueList(oldVersion, newVersion, subject.category_history)
		val properties = subject.properties_history.mapValues(createValueList(oldVersion, newVersion, _))
		val relations = subject.relations_history.mapValues(_.mapValues(createValueList(oldVersion, newVersion, _)))
		HistoryEntry(subject.id, nameList, aliasesList, categoryList, properties, relations)
	}

	/**
	  * Creates a diff of the values in the History Entry and writes it to a JSON object.
	  * @param entry History Entry containing the data of two versions
	  * @return JSON object containing the diff
	  */
	def diffToJson(entry: HistoryEntry): JsValue = {
		val jsonObject = mutable.Map[String, JsValue]()
		jsonObject("id") = Json.toJson(entry.id)
		diffLists(entry.name).foreach(name => jsonObject("name") = name)
		diffLists(entry.aliases).foreach(aliases => jsonObject("aliases") = aliases)
		diffLists(entry.category).foreach(category => jsonObject("category") = category)
		val propertyDiff = entry.properties.mapValues(diffLists).filter(_._2.nonEmpty)
		if(propertyDiff.nonEmpty) jsonObject("properties") = Json.toJson(propertyDiff)
		val relationsMap = entry.relations
			.mapValues(_.mapValues(diffLists))
			.map(_.map(_.toString, _.filter(_._2.nonEmpty).mapValues(_.get)))
			.filter(_._2.nonEmpty)
			.mapValues(Json.toJson(_))
		if(relationsMap.nonEmpty) jsonObject("relations") = Json.toJson(relationsMap)
		Json.toJson(jsonObject.toMap)
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

	/**
	  * Creates a diff for each subject containing the deletions and additions of every field between the two versions.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val (oldVersion, newVersion) = versionOrder(UUID.fromString(args(0)), UUID.fromString(args(1)))
		input.fromAnyRDD[Subject]()
			.map(rdd =>
				rdd
					.map(retrieveVersions(_, oldVersion, newVersion))
					.map(diffToJson))
			.toAnyRDD()
	}

	/**
	  * Asserts that two versions are given as program arguments.
	  * @param args arguments of the program
	  * @return true if there are at least two arguments provided
	  */
	override def assertConditions(args: Array[String]): Boolean = {
		args.length >= 2
	}
}
