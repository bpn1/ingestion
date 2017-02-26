import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import DataLake.{Subject, SubjectManager, Version}
import com.datastax.driver.core.utils.UUIDs
import org.apache.spark.rdd.RDD

object DBPediaDeduplication {

	val appName = "DBPediaDeduplication_v0.1"
	val dataSources = List("dbpedia_20161203")
	val outputKeyspace = "datalake"
	val outputTable = "duplicatecandidates"

	def makeTemplateVersion(): Version = {
		// create timestamp and TimeUUID for versioning
		val timestamp = new Date()
		val version = UUIDs.timeBased()

		Version(version, appName, null, null, dataSources, timestamp)
	}

	def dbpediaFilter(rdd: RDD[Subject]): RDD[(String, Subject)] = {
		rdd
			.filter(_.name.isDefined)
			.keyBy(_.name.get)
			// To remove the @de  . from the name of every dbpedia subject
			.map(x => (x._1.dropRight(5), x._2))
	}

	def subjectFilter(rdd: RDD[Subject]): RDD[(String, Subject)] = {
		rdd
		  .filter(_.properties.contains("wikipedia_name"))
			.keyBy(_.properties.getOrElse("wikipedia_name", List("null")).head)
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
		val dbpedia = sc.cassandraTable[Subject]("datalake", "subject_temp")
		val subjects = sc.cassandraTable[Subject]("datalake", "subject")
		val dbpediaTopic = dbpediaFilter(dbpedia)
		val subjectTopic = subjectFilter(subjects)

		val joined : RDD[Subject] = dbpediaTopic
			.join(subjectTopic)
		  .map(x => mergeSubjects(x._2._1, x._2._2, version))

		joined.saveToCassandra(outputKeyspace, outputTable)

		sc.stop()
	}
}