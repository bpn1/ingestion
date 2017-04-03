package de.hpi.ingestion.dataimport.wikidata

import org.apache.spark.{SparkConf, SparkContext}
import java.util.{Date, UUID}
import scala.collection.mutable
import scala.util.matching.Regex
import com.datastax.spark.connector._
import com.datastax.driver.core.utils.UUIDs
import de.hpi.ingestion.dataimport.wikidata.models.WikiDataEntity
import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.SubjectManager

object FindRelations {
	val appname = "FindRelations_v1.1"
	val datasources = List("wikidata_20161117")

	val keyspace = "datalake"
	val tablename = "subject"
	val versionTablename = "version"

	val wikiDataIdKey = "wikidata_id"

	def findRelations(
		subject: Subject,
		idMap: Map[String, (UUID, String)],
		version: Version): Subject =
	{
		val sm = new SubjectManager(subject, version)
		val relationsBuffer = mutable.Map[UUID, Map[String, String]]()

		val idRegex = new Regex("^Q[0-9]+$")

		val propertyBuffer = mutable.Map[String, List[String]]()
		propertyBuffer ++= subject.properties

		for((key, list) <- subject.properties) {
			if(key != wikiDataIdKey) {
				val updatedList = mutable.ListBuffer[String]()

				for(value <- list) {
					if(idRegex.findFirstIn(value).isDefined && idMap.contains(value)) {
						val idTuple = idMap(value)
						relationsBuffer ++= Map(idTuple._1 -> Map("type" -> key))

						// translate ID to label (if available)
						val label = idTuple._2
						if(label != "") {
							updatedList.append(label)
						} else {
							updatedList.append(value)
						}
					} else {
						updatedList.append(value) // keep original values
					}
				}

				propertyBuffer(key) = updatedList.toList
			}
		}

		sm.addProperties(propertyBuffer.toMap)
		sm.addRelations(relationsBuffer.toMap)

		subject
	}

	def makeTemplateVersion(): Version = {
		// create timestamp and TimeUUID for versioning
		val timestamp = new Date()
		val version = UUIDs.timeBased()

		Version(version, appname, null, null, datasources, timestamp)
	}

	def main(args : Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName(appname)
			.set("spark.cassandra.connection.host", "odin01")

		val sc = new SparkContext(conf)
		val subjects = sc.cassandraTable[Subject](keyspace, tablename)

		val idMap = subjects
			.map(s => (s.properties(wikiDataIdKey).head, (s.id, s.name.getOrElse(""))))
			.collect
			.toMap

		val version = makeTemplateVersion()

		val lakeData = subjects
			.map(findRelations(_, idMap, version))
			.saveToCassandra(keyspace, tablename)

		// write version information
		val versionRDD = sc.parallelize(
			List((version.version, version.timestamp, version.datasources, version.program)))
		versionRDD.saveToCassandra(
			keyspace,
			versionTablename,
			SomeColumns("version", "timestamp", "datasources", "program"))
	}
}
