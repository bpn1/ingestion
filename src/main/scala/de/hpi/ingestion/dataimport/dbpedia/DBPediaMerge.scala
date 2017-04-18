package de.hpi.ingestion.dataimport.dbpedia

import java.util.{Date, UUID}
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.models.DuplicateCandidates
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.SubjectManager

/**
  * Merge-Job to merge DBPedia-Subjects and WikiData-Subjects.
  */
object DBPediaMerge {
	val appName = "DBPediaMerge_v0.1"
	val dataSources = List("dbpedia_20161203")
	val keyspace = "datalake"
	val duplicateTable = "duplicatecandidates"
	val dbpediaTable = "subject_temp"
	val subjectTable = "subject"
	val outputTable = "subject_merged"

	/**
	  * Merges a Subjects with its duplicates.
	  * @param subject Subject to be merged
	  * @param duplicates List of Subject declared as duplicates of the Subject
	  * @param version Version (of the execution) used for versioning of the attributes
	  * @return Subject containing the merged data of the original Subject and the duplicates
	  */
	def mergeSubjects(subject: Subject, duplicates: List[Subject], version: Version): Subject = {
		val sm = new SubjectManager(subject, version)
		val prefixedProperties = subject
			.properties
			.map { case (key, value) => s"wikidata.$key" -> value }

		val newProperties = duplicates
			.foldLeft(Map[String, List[String]]()) {
				(properties, subject) => properties ++ subject.toProperties("dbpedia")
			}

		sm.removeProperties()
		sm.addProperties(prefixedProperties ++ newProperties)
		subject
	}

	/**
	  * Joins Subjects with their DuplicateCandidates.
	  * @param subjects RDD of Subjects
	  * @param duplicates RDD of DuplicateCandidates
	  * @return RDD containing Subjects and a list of its duplicates
	  */
	def joinSubjectsWithDuplicates(
		subjects: RDD[Subject],
		duplicates: RDD[DuplicateCandidates]
	): RDD[(Subject, List[Subject])] = {
		subjects
			.keyBy(_.id)
			.join(duplicates.keyBy(_.subject_id))
			.values
			.map { case (subject, candidates) => (subject, candidates.candidates.map(_._1)) }
	}

	/**
	  * Extracts the UUIDs of all Subjects declared as duplicates.
	  * @param duplicates RDD of DuplicateCandidates
	  * @return List of the Subjects UUIDs
	  */
	def extractIds(duplicates: RDD[DuplicateCandidates]): List[UUID] = {
		duplicates
			.map(_.candidates.map(_._1.id))
			.reduce(_ ::: _)
			.distinct
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName(appName)

		val sc = new SparkContext(conf)

		val version = Version(appName, dataSources, sc)
		val subjects = sc.cassandraTable[Subject](keyspace, subjectTable)
		val dbpedia = sc.cassandraTable[Subject](keyspace, dbpediaTable)
		val duplicates = sc.cassandraTable[DuplicateCandidates](keyspace, duplicateTable)

		val duplicateIds = extractIds(duplicates)

		val notDuplicates = dbpedia
			.filter(subject => !duplicateIds.contains(subject.id))

		val joinedSubjects = joinSubjectsWithDuplicates(subjects, duplicates)
			.map(x => mergeSubjects(x._1, x._2, version))
			.union(notDuplicates)

		joinedSubjects.saveToCassandra(keyspace, outputTable)
	}
}
