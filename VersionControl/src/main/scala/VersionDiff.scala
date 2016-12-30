import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import java.util.{UUID, Date}

object VersionDiff {
	val keyspace = "datalake"
	val tablename = "subject"
	val csvSeparator = ","
	val diffListSeparator = ";"
	val NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

	def timeFromUUID(uuid : UUID): Long = {
		// from https://github.com/rantav/hector/blob/master/core/src/main/java/me/prettyprint/cassandra/utils/TimeUUIDUtils.java
		(uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000
	}

	// returns list of values of the given version or the last version before it that changed the given field
	def findValues(queryVersion : UUID, versionList : List[Version]) : List[String] = {
		// check if our version exists in the list
		if(versionList.filter(_.version == queryVersion).length > 0) {
			versionList.filter(_.version == queryVersion).head.value
		} else {
			// returns the last version of all versions older than the one we look for
			val olderVersions = versionList.filter(v => timeFromUUID(v.version) < timeFromUUID(queryVersion))
			if(olderVersions.length > 0) {
				olderVersions.maxBy(v => timeFromUUID(v.version)).value
			} else {
				// there is no older version for this value
				List[String]()
			}
		}
	}

	// returns a list of old and new values for the given field
	// returns null if the current field has no versions
	def createValueList(oldVersion : UUID, newVersion : UUID, versionList : List[Version]) : List[List[String]] = {
		if(versionList.length > 0) {
			List(findValues(oldVersion, versionList), findValues(newVersion, versionList))
		} else {
			null
		}
	}

	// returns an empty string if there were no versions no compare and otherwise returns a string in the following format
	// -oldValue1;oldValue2;oldValue3 +newValue1;newValue2;newValue3
	def diffLists(valueList : List[List[String]]) : String = {
		if(valueList == null)
			return ""
		val oldValueList = valueList(0)
		val newValueList = valueList(1)
		val oldValues = oldValueList.filterNot(newValueList.toSet)
		val newValues = newValueList.filterNot(oldValueList.toSet)
		"-" + oldValues.mkString(diffListSeparator) + " +" + newValues.mkString(diffListSeparator)
	}

	def main(args : Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("VersionDiff")
			.set("spark.cassandra.connection.host", "172.20.21.11")
		val sc = new SparkContext(conf)

		// stop if we don't have two versions to compare
		if(args.length < 2)
            return

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
			// map version histories to list of old and new values in the following format
			// List(List(oldValue1, oldValue2), List(newValue1, newValue2))
			.map { case (id, name_h, aliases_h, category_h, properties_h, relations_h) => {
					val nameList = createValueList(oldVersion, newVersion, name_h)
					val aliasesList = createValueList(oldVersion, newVersion, aliases_h)
					val categoryList = createValueList(oldVersion, newVersion, category_h)
					val properties = properties_h.mapValues(versionList =>
						createValueList(oldVersion, newVersion, versionList))
					val relations = relations_h.mapValues(_.mapValues(versionList =>
							createValueList(oldVersion, newVersion, versionList)))
					(id, nameList, aliasesList, categoryList, properties, relations)
				}
			// diffs the value lists and parses them into strings
			}.map { case (id, nameList, aliasesList, categoryList, properties, relations) => {
					val nameDiff = diffLists(nameList)
					val aliasesDiff = diffLists(aliasesList)
					val categoryDiff = diffLists(categoryList)
					val propertiesDiff = properties.mapValues(diffLists)
					val relationsDiff = relations.mapValues(_.mapValues(diffLists))
					(id, nameDiff, aliasesDiff, categoryDiff, propertiesDiff, relationsDiff)
				}
			// creates csv string with fields being encapsulated in quotation marks ("")
			}.map { case (id, nameDiff, aliasesDiff, categoryDiff, propertiesDiff, relationsDiff) =>
				List(id, nameDiff, aliasesDiff, categoryDiff,
					propertiesDiff, relationsDiff).map("\"" + _ + "\"").mkString(csvSeparator)
			}.saveAsTextFile("versionDiff_" + System.currentTimeMillis / 1000)
    }
}
