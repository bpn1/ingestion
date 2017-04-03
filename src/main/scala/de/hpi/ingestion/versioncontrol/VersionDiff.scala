package de.hpi.ingestion.versioncontrol

import de.hpi.ingestion.datalake.models._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import java.util.UUID
import play.api.libs.json._
import scala.collection.mutable

object VersionDiff {
	val keyspace = "datalake"
	val tablename = "subject"
	val NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

	def timeFromUUID(uuid : UUID): Long = {
		// scalastyle:off line.size.limit
		// from https://github.com/rantav/hector/blob/master/core/src/main/java/me/prettyprint/cassandra/utils/TimeUUIDUtils.java
		// scalastyle:on line.size.limit
		(uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000
	}

	// returns list of values of the given version
	// or the last version before it that changed the given field
	def findValues(queryVersion : UUID, versionList : List[Version]) : List[String] = {
		// check if our version exists in the list
		if(versionList.exists(_.version == queryVersion)) {
			versionList.filter(_.version == queryVersion).head.value
		} else {
			// returns the last version of all versions older than the one we look for
			val olderVersions = versionList
				.filter(v => timeFromUUID(v.version) < timeFromUUID(queryVersion))
			if(olderVersions.nonEmpty) {
				olderVersions.maxBy(v => timeFromUUID(v.version)).value
			} else {
				// there is no older version for this value
				List[String]()
			}
		}
	}

	// returns a list of old and new values for the given field
	// returns null if the current field has no versions
	def createValueList(
		oldVersion : UUID,
		newVersion : UUID,
		versionList : List[Version]) : List[List[String]] =
	{
		if(versionList.nonEmpty) {
			List(findValues(oldVersion, versionList), findValues(newVersion, versionList))
		} else {
			null
		}
	}

	// returns an empty string if there were no versions no compare
	// otherwise returns a string in the following format
	// -oldValue1;oldValue2;oldValue3 +newValue1;newValue2;newValue3
	def diffLists(valueList : List[List[String]]) : JsValue = {
		// if input does not exist
		if(valueList == null) {
			return null
		}
		val result = mutable.Map[String, JsValue]()
		val oldValueList = valueList(0)
		val newValueList = valueList(1)
		val oldValues = oldValueList.filterNot(newValueList.toSet)
		val newValues = newValueList.filterNot(oldValueList.toSet)
		if(oldValues.nonEmpty) {
			result += Tuple2("-", Json.toJson(oldValues))
		}
		if(newValues.nonEmpty) {
			result += Tuple2("+", Json.toJson(newValues))
		}
		if(result.isEmpty) {
			return null
		}
		Json.toJson(result.toMap)
	}

	// map version histories to list of old and new values in the following format
	// List(List(oldValue1, oldValue2), List(newValue1, newValue2))
	def retrieveVersions(
		entry : (UUID,
			List[Version],
			List[Version],
			List[Version],
			Map[String, List[Version]],
			Map[UUID, Map[String, List[Version]]]),
		oldVersion : UUID,
		newVersion : UUID
	) : (UUID,
		List[List[String]],
		List[List[String]],
		List[List[String]],
		Map[String, List[List[String]]],
		Map[UUID, Map[String, List[List[String]]]]) =
	{
		entry match {
			case (id, name_h, aliases_h, category_h, properties_h, relations_h) => {
					val nameList = createValueList(oldVersion, newVersion, name_h)
					val aliasesList = createValueList(oldVersion, newVersion, aliases_h)
					val categoryList = createValueList(oldVersion, newVersion, category_h)
					val properties = properties_h.mapValues(versionList =>
						createValueList(oldVersion, newVersion, versionList))
					val relations = relations_h.mapValues(_.mapValues(versionList =>
							createValueList(oldVersion, newVersion, versionList)))
					(id, nameList, aliasesList, categoryList, properties, relations)
			}
		}
	}

	// diffs the value lists and parses them into Json
	def diffToJson(
		entry : (UUID,
			List[List[String]],
			List[List[String]],
			List[List[String]],
			Map[String, List[List[String]]],
			Map[UUID, Map[String, List[List[String]]]])
	) : Map[String, JsValue] = {
		entry match {
			case (id, nameList, aliasesList, categoryList, properties, relations) =>
				Map(
					"id" -> Json.toJson(id),
					"name" -> diffLists(nameList),
					"aliases" -> diffLists(aliasesList),
					"category" -> diffLists(categoryList),
					"properties" -> Json.toJson(properties
						.mapValues(diffLists)
						.filter(_._2 != null)),
					"relations" -> Json.toJson(relations
						.map { case (key, value) =>
							(key.toString, value.mapValues(diffLists).filter(_._2 != null))
						}.filter(_._2.nonEmpty)
						.mapValues(propMap => Json.toJson(propMap)))
				).filter(_._2 != null)
		}
	}

	def main(args : Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("VersionDiff")
		val sc = new SparkContext(conf)

		// stop if we don't have two versions to compare
		if(args.length < 2) {
			return
		}

		// identify older and newer version
		var oldVersion = UUID.fromString(args(0))
		var newVersion = UUID.fromString(args(1))
		if(timeFromUUID(oldVersion) > timeFromUUID(newVersion)) {
			oldVersion = UUID.fromString(args(1))
			newVersion = UUID.fromString(args(0))
		}

    	val subjects = sc.cassandraTable[Subject](keyspace, tablename)
		subjects
			// remove value columns
			.map(subject => (subject.id, subject.name_history,
				subject.aliases_history, subject.category_history,
				subject.properties_history, subject.relations_history))
			.map(retrieveVersions(_, oldVersion, newVersion))
			.map(diffToJson)
			.map(diffMap => Json.toJson(diffMap))
			.saveAsTextFile("versionDiff_" + System.currentTimeMillis / 1000)
		sc.stop
    }
}
