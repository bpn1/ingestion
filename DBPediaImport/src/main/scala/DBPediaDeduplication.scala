import java.util.{Date, UUID}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import DataLake.{Subject, SubjectManager, Version}
import com.datastax.driver.core.utils.UUIDs
import org.apache.spark.rdd.RDD

object DBPediaDeduplication {

	val appName = "DBPediaDeduplication_v0.1"
	val dataSources = List("dbpedia_20161203")
	val keyspace = "datalake"
	val inputTableName = "subject_temp"
	val subjectTable = "subject"
	val wikidataKey = "wikipedia_name"
	val outputTable = "duplicatecandidates"

	case class DuplicateCandidate(
		id: UUID,
		subject_id: UUID,
		subject_name: Option[String] = None,
		duplicate_id: UUID,
		duplicate_name: Option[String] = None,
		duplicate_table: String)

	def makeTemplateVersion(): Version = {
		// create timestamp and TimeUUID for versioning
		val timestamp = new Date()
		val version = UUIDs.timeBased()

		Version(version, appName, List[String](), Map[String, String](), dataSources, timestamp)
	}

	def dbpediaFilter(rdd: RDD[Subject]): RDD[(String, Subject)] = {
		rdd
			.filter(_.name.isDefined)
			.map { entry =>
				// To remove the @de  . from the name of every dbpedia subject
				val name = entry.name.get.dropRight(5)
				(name, entry)
			}
	}

	def subjectFilter(rdd: RDD[Subject]): RDD[(String, Subject)] = {
		rdd
			.filter(_.properties.contains(wikidataKey))
			.map { entry =>
				val keyOpt = entry.properties(wikidataKey).headOption
				(keyOpt.getOrElse(""), entry)
			}
	}

	def mergeSubjects(subject1: Subject, subject2: Subject, version: Version) : Subject = {
		val sm = new SubjectManager(subject1, version)
		sm.addProperties(subject2.properties)
		subject1
	}

	def main(args: Array[String]) {
		val conf = new SparkConf()
			.setAppName(appName)
			.set("spark.cassandra.connection.host", "odin01")

		val sc = new SparkContext(conf)

		val version = makeTemplateVersion()
		val dbpedia = sc.cassandraTable[Subject](keyspace, inputTableName)
		val subjects = sc.cassandraTable[Subject](keyspace, subjectTable)
		val dbpediaTopic = dbpediaFilter(dbpedia)
		val subjectTopic = subjectFilter(subjects)

		dbpediaTopic
			.join(subjectTopic)
			.map { case (key, (dbpediaSubject, subject)) =>
				DuplicateCandidate(
					UUIDs.random(),
					subject.id,
					subject.name,
					dbpediaSubject.id,
					dbpediaSubject.name,
					inputTableName)
			}.saveToCassandra(keyspace, outputTable)


		sc.stop()
	}
}