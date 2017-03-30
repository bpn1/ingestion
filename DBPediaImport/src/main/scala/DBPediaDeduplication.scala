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

	def keyBySingleProperty(rdd: RDD[Subject], property: String): RDD[(String, Subject)] = {
		rdd
		  .filter(_.properties.contains(property))
		  .map { subject =>
				val keyOpt = subject.properties(property).headOption
				(keyOpt.getOrElse(""), subject)
			}
	}

	def keyByProperty(rdd: RDD[Subject], property: String): RDD[(List[String], Subject)] = {
		rdd
		  .filter(_.properties.contains(property))
		  .map { subject =>
				val list = subject.properties.getOrElse(property, List())
				(list, subject)
			}
	}

	def translateToCandidate(rdd: RDD[(String, (Subject, Subject))]): RDD[DuplicateCandidate] = {
		rdd
		  .map { case (key, (subject1, subject2)) =>
				DuplicateCandidate(
					UUIDs.random(),
					subject1.id,
					subject1.name,
					subject2.id,
					subject2.name,
					inputTableName)
			}
	}

	def joinOnName(dbpediaRDD: RDD[Subject], subjectRDD: RDD[Subject]): RDD[(String, (Subject, Subject))] = {
		val dbpediaNameRDD = dbpediaRDD
		  .filter(_.name.isDefined)
		  .map { subject =>
				// To remove the @de  . from the name of every dbpedia subject
				val name = subject.name.get.dropRight(5)
				(name, subject)
			}

		val subjectWikinameRDD = keyBySingleProperty(subjectRDD, "wikipedia_name")

		dbpediaNameRDD.join(subjectWikinameRDD)
	}

	def joinOnWikiId(dbpediaRDD: RDD[Subject], subjectRDD: RDD[Subject]): RDD[(String, (Subject, Subject))] = {
		val dbpediaSameAsRDD = keyByProperty(dbpediaRDD, "owl:sameAs")
		  .filter(_._1.nonEmpty)
			.filter(_._1.exists(_.startsWith("wikidata:")))
		  .map { case (property, subject) =>
				val id = property
					.filter(_.startsWith("wikidata:"))
				  .head
				  .drop(9)
				(id, subject)
			}

		val subjectWikiId = keyBySingleProperty(subjectRDD, "wikidata_id")

		dbpediaSameAsRDD.join(subjectWikiId)
	}

	def main(args: Array[String]) {
		val conf = new SparkConf()
			.setAppName(appName)
			.set("spark.cassandra.connection.host", "odin01")

		val sc = new SparkContext(conf)

		val version = makeTemplateVersion()
		val dbpedia = sc.cassandraTable[Subject](keyspace, inputTableName)
		val subjects = sc.cassandraTable[Subject](keyspace, subjectTable)

		val nameJoined = joinOnName(dbpedia, subjects)
		val idJoined = joinOnWikiId(dbpedia, subjects)

		val nameCandidates = translateToCandidate(nameJoined)
		val idCandidates = translateToCandidate(idJoined)

		println(nameCandidates.count())
		println(idCandidates.count())

		println(nameCandidates
		  .keyBy(duplicate => (duplicate.subject_id, duplicate.duplicate_id))
		  .join(idCandidates.keyBy(duplicate => (duplicate.subject_id, duplicate.duplicate_id)))
		  .count())

		sc.stop()
	}
}
