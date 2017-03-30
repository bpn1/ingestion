import org.apache.spark.{SparkConf, SparkContext}
import java.util.{Date, UUID}

import scala.collection.mutable
import scala.util.matching.Regex
import com.datastax.spark.connector._
import com.datastax.driver.core.utils.UUIDs
import DataLake.{Subject, SubjectManager, Version}
import org.apache.spark.rdd.RDD

object FindRelations {
	val appname = "FindRelations_v1.1"
	val datasources = List("wikidata_20161117")
	val keyspace = "datalake"
	val tablename = "subject"
	val versionTablename = "version"
	val wikiDataIdKey = "wikidata_id"

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
		val relationsMap = mutable.Map[UUID, Map[String, String]]()
		val relationTypeKey = "type"
		val idRegex = new Regex("^Q[0-9]+$")
		val propertyMap = mutable.Map() ++ subject.properties

		subject.properties
			.filter(_._1 != wikiDataIdKey)
		    .foreach { case (key, list) =>
				val updatedList = mutable.ListBuffer[String]()
				// skip properties without wikidata id or without resolve entry
				val partitions = list.partition(value =>
					idRegex.findFirstIn(value).isDefined && nameResolveMap.contains(value))
				updatedList ++= partitions._2

				// add relations to subject with the corresponding wikidata id
				val propertyValues = partitions._1
				relationsMap ++= propertyValues.map(value =>
					(nameResolveMap(value)._1, Map(relationTypeKey -> key))).toMap

				// append resolved name to property value if it exists
				updatedList ++= propertyValues
					.filter(nameResolveMap(_)._2.nonEmpty)
					.map(nameResolveMap(_)._2)
				propertyMap(key) = updatedList.toList
			}

		sm.addProperties(propertyMap.toMap)
		sm.addRelations(relationsMap.toMap)
		subject
	}

	/**
	  * Creates template version for versioning. This template contains data for the fields
	  * version, program, datasources and timestamp.
	  * @return template version
	  */
	def makeTemplateVersion(): Version = {
		val timestamp = new Date()
		val version = UUIDs.timeBased()

		Version(version, appname, List[String](), Map[String, String](), datasources, timestamp)
	}

	/**
	  * Writes version to the Cassandra version table.
	  * @param version Version to write to the Cassandra.
	  * @param sc spark context to use for database access
	  */
	def writeVersion(version: Version, sc: SparkContext): Unit = {
		val versionRDD = sc.parallelize(
			List((version.version, version.timestamp, version.datasources, version.program)))

		versionRDD.saveToCassandra(
			keyspace,
			versionTablename,
			SomeColumns("version", "timestamp", "datasources", "program"))
	}

	/**
	  * Creates map containing the wikidata id as key and the corresponding subjects id and name
	  * as value.
	  * @param subjects RDD of subjects
	  * @return map of wikidata ids mapping to the corresponding subject id and name
	  */
	def resolvableNamesMap(subjects: RDD[Subject]): Map[String, (UUID, String)] = {
		subjects
			.filter(subject => subject.properties.contains(wikiDataIdKey)
					&& subject.properties(wikiDataIdKey).nonEmpty)
			.map { subject =>
				val wikidataId = subject.properties(wikiDataIdKey).head
				val name = subject.name.getOrElse(subject.aliases.headOption.getOrElse(wikidataId))
				(wikidataId, (subject.id, name))
			}.collect
			.toMap
	}

	def main(args : Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName(appname)

		val sc = new SparkContext(conf)
		val subjects = sc.cassandraTable[Subject](keyspace, tablename)

		val nameResolveMap = resolvableNamesMap(subjects)

		val version = makeTemplateVersion()
		subjects
			.map(findRelations(_, nameResolveMap, version))
			.saveToCassandra(keyspace, tablename)
		writeVersion(version, sc)
	}
}
