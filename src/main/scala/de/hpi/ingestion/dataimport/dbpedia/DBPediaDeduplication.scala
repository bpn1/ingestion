package de.hpi.ingestion.dataimport.dbpedia

import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.dataimport.dbpedia.models.DuplicateCandidates
import java.util.{Date, UUID}
import org.apache.spark.rdd.RDD
import com.datastax.driver.core.utils.UUIDs

object DBPediaDeduplication {

	val appName = "DBPediaDeduplication_v0.1"
	val dataSources = List("dbpedia_20161203")
	val keyspace = "datalake"
	val inputTableName = "subject_temp"
	val subjectTable = "subject"
	val outputTable = "goldenstandard"

	case class DuplicateCandidate(
		subject_id: UUID,
		duplicate_id: UUID,
		duplicate_table: String
	){
		override def hashCode(): Int = (this.subject_id, this.duplicate_id).hashCode()

		override def equals(obj: Any): Boolean = obj match {
			case that: DuplicateCandidate => {
				that.subject_id == this.subject_id && that.duplicate_id == this.duplicate_id
			}
			case _ => false
		}

		def toTuple(): (UUID, UUID, String) = {
			(this.subject_id, this.duplicate_id, this.duplicate_table)
		}
	}

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

	def translateToCandidate(
		rdd: RDD[(String, (Subject, Subject))],
		duplicate_table: String
	): RDD[DuplicateCandidate] = {
		rdd
		  .map { case (key, (subject1, subject2)) =>
				DuplicateCandidate(
					subject1.id,
					subject2.id,
					duplicate_table
				)
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

	def joinGoldenStandard(
		dbpediaRDD: RDD[(String, (Subject, Subject))],
		subjectRDD: RDD[(String, (Subject, Subject))]
	): RDD[Subject] = {
		val left = dbpediaRDD.values
		val right = subjectRDD.values

		left
			.union(right)
		    .flatMap(x => List(x._1, x._2))
		    .distinct()
	}

	def joinCandidates(
		candidate1: RDD[DuplicateCandidate],
		candidate2: RDD[DuplicateCandidate]
	): RDD[DuplicateCandidates] = {
		val left = candidate1
		  .keyBy(duplicate => duplicate.subject_id)

		val right = candidate2
			.keyBy(duplicate => duplicate.subject_id)

		left
		    .cogroup(right)
		    .map {
				case (subject, (it1, it2)) => DuplicateCandidates(
					subject = subject,
					candidates = Some(it1.++(it2).toList.distinct.map(_.toTuple()))
				)
			}
	}

	def main(args: Array[String]) {
		val conf = new SparkConf()
			.setAppName(appName)

		val sc = new SparkContext(conf)

		val version = makeTemplateVersion()
		val dbpedia = sc.cassandraTable[Subject](keyspace, inputTableName)
		val subjects = sc.cassandraTable[Subject](keyspace, subjectTable)

		val nameJoined = joinOnName(dbpedia, subjects)
		val idJoined = joinOnWikiId(dbpedia, subjects)

		joinGoldenStandard(nameJoined, idJoined).saveToCassandra(keyspace, outputTable)

		//val nameCandidates = translateToCandidate(nameJoined, inputTableName)
		//val idCandidates = translateToCandidate(idJoined, inputTableName)

		//joinCandidates(nameCandidates, idCandidates).saveToCassandra(keyspace, outputTable)

		sc.stop()
	}
}
