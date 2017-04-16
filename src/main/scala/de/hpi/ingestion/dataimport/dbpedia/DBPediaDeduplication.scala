package de.hpi.ingestion.dataimport.dbpedia

import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.models.DuplicateCandidates
import java.util.UUID
import org.apache.spark.rdd.RDD

/**
  * Deduplication-Job for merging DBPedia with WikiData subjects.
  */
object DBPediaDeduplication {

	val appName = "DBPediaDeduplication_v0.1"
	val dataSources = List("dbpedia_20161203")
	val keyspace = "datalake"
	val inputTableName = "subject_temp"
	val subjectTable = "subject"
	val outputTable = "duplicatecandidates"

	/**
	  * A candidate of a possible duplicate pair.
	  * @param subject_id Id of the Subject
	  * @param duplicate Possible duplicate of the Subject
	  * @param duplicate_table Source of the new Subject
	  * @param score Reliabilty score
	  */
	case class DuplicateCandidate(
		subject_id : UUID,
		duplicate: Subject,
		duplicate_table: String,
		score: Double = 1.0
	){
		/**
		  * Converts the duplicate candidate into a tuple
		  * @return 3-Tuple containing the candidate, source table and a score
		  */
		def toTuple(): (Subject, String, Double) = {
			(this.duplicate, this.duplicate_table, this.score)
		}
	}

	/**
	  * Keys Subjects by a property value
	  * @param rdd Subjects
	  * @param property Property to key by
	  * @return RDD containing Subjects keyed by a value
	  */
	def keyBySingleProperty(rdd: RDD[Subject], property: String): RDD[(String, Subject)] = {
		rdd
			.filter(_.properties.contains(property))
		  	.map { subject =>
				val keyOpt = subject.properties(property).headOption
				(keyOpt.getOrElse(""), subject)
			}
	}

	/**
	  * Keys Subjects by a property value list
	  * @param rdd RDD of Subjects
	  * @param property property used to key the subjects
	  * @return RDD containing 2-Tuples of Subjects keyed by a list of values in the form (valueList, Subject)
	  */
	def keyByProperty(rdd: RDD[Subject], property: String): RDD[(List[String], Subject)] = {
		rdd
		  	.filter(_.properties.contains(property))
		  	.map { subject =>
				val list = subject.properties(property)
				(list, subject)
			}
	}

	/**
	  * Translates a possible duplicate pair into a DuplicateCandidate
	  * @param candidate The duplicate pair (Subject, possible duplicate)
	  * @param duplicate_table Source table of the possible duplicate
	  * @return DuplicateCandidate of the duplicate pair
	  */
	def translateToCandidate(
		candidate: (String, (Subject, Subject)),
		duplicate_table: String
	): DuplicateCandidate = {
		val (key, (subject1, subject2)) = candidate
		DuplicateCandidate(subject1.id, subject2, duplicate_table)
	}

	/**
	  * Joins DBPedia-Subjects with WikiData-Subjects on the wikipedia name
	  * @param dbpediaRDD DBPedia-Subjects
	  * @param subjectRDD WikiData-Subjects
	  * @return RDD containing the joined subject pairs
	  */
	def joinOnName(dbpediaRDD: RDD[Subject], subjectRDD: RDD[Subject]): RDD[(String, (Subject, Subject))] = {
		val dbpediaNameRDD = dbpediaRDD
		  	.filter(_.name.isDefined)
		  	.map { subject =>
				(subject.name.get, subject)
			}

		val subjectWikinameRDD = keyBySingleProperty(subjectRDD, "wikipedia_name")

		dbpediaNameRDD.join(subjectWikinameRDD)
	}

	/**
	  * Joins DBPedia-Subjects with WikiData-Subjects on the wikidata id
	  * @param dbpediaRDD DBPedia-Subjects
	  * @param subjectRDD WikiData-Subjects
	  * @return RDD containing the joined subject pairs
	  */
	def joinOnWikiId(dbpediaRDD: RDD[Subject], subjectRDD: RDD[Subject]): RDD[(String, (Subject, Subject))] = {
		val dbpediaSameAsRDD = keyBySingleProperty(dbpediaRDD, "wikidata_id")
		val subjectWikiId = keyBySingleProperty(subjectRDD, "wikidata_id")

		dbpediaSameAsRDD.join(subjectWikiId)
	}

	/**
	  * Joins all duplicates found in one RDD
	  * @param dbpediaRDD DBPedia-Subjects
	  * @param subjectRDD WikiData-Subjects
	  * @return RDD containing Subjects and their duplicates
	  */
	def joinGoldStandard(
		dbpediaRDD: RDD[(String, (Subject, Subject))],
		subjectRDD: RDD[(String, (Subject, Subject))]
	): RDD[Subject] = {
		dbpediaRDD
		    .union(subjectRDD)
		    .flatMap(x => List(x._2._1, x._2._2))
		    .distinct
	}

	/**
	  * Joins two RDDs of DuplicateCandidate together
	  * @param candidate1 First Candiate RDD
	  * @param candidate2 Second Candidate RDD
	  * @return RDD containing DuplicateCandidates
	  */
	def joinCandidates(
		candidate1: RDD[DuplicateCandidate],
		candidate2: RDD[DuplicateCandidate]
	): RDD[DuplicateCandidates] = {
		val keyedCandidate1 = candidate1.keyBy(_.subject_id)
		val keyedCandidate2 = candidate2.keyBy(_.subject_id)

		keyedCandidate1
			.cogroup(keyedCandidate2)
			.filter { case (subject_id, (it1, it2)) => (it1 ++ it2).nonEmpty }
			.map {
				case (subject_id, (it1, it2)) => DuplicateCandidates(
					subject_id,
					(it1 ++ it2).toList.distinct.map(_.toTuple())
				)
			}
	}

	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName(appName)
		val sc = new SparkContext(conf)

		val version = Version(appName, dataSources, sc)
		val dbpedia = sc.cassandraTable[Subject](keyspace, inputTableName)
		val subjects = sc.cassandraTable[Subject](keyspace, subjectTable)

		val nameJoined = joinOnName(dbpedia, subjects)
		val idJoined = joinOnWikiId(dbpedia, subjects)

		val nameCandidates = nameJoined.map(translateToCandidate(_, inputTableName))
		val idCandidates = idJoined.map(translateToCandidate(_, inputTableName))

		joinGoldStandard(nameJoined, idJoined).saveToCassandra(keyspace, "goldstandard")
		joinCandidates(nameCandidates, idCandidates).saveToCassandra(keyspace, outputTable)

		sc.stop()
	}
}