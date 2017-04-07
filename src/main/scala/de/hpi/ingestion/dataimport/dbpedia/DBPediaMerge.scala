package de.hpi.ingestion.dataimport.dbpedia

import java.util.Date

import com.datastax.driver.core.utils.UUIDs
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.models.DuplicateCandidates
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.SubjectManager
import scala.collection.mutable

object DBPediaMerge {
	val appName = "DBPediaMerge_v0.1"
	val dataSources = List("dbpedia_20161203")
	val keyspace = "datalake"
	val duplicateTable = "duplicatecandidates"
	val dbpediaTable = "subject_temp"
	val subjectTable = "subject"
	val outputTable = "subject_merged"

	def makeTemplateVersion(): Version = {
		// create timestamp and TimeUUID for versioning
		val timestamp = new Date()
		val version = UUIDs.timeBased()

		Version(version, appName, List[String](), Map[String, String](), dataSources, timestamp)
	}

	def reduceSubject(subject: Subject): Map[String, List[String]] = {
		val properties = mutable.Map[String, List[String]]()

		if(subject.name.isDefined) properties += ("dbpedia.name" -> subject.get("name"))
		if(subject.aliases.nonEmpty) properties += ("dbpedia.aliases" -> subject.get("aliases"))
		if(subject.category.isDefined) properties += ("dbpedia.category" -> subject.get("category"))

		val prefixedProperties : Map[String, List[String]] = subject
			.properties
			.keySet
			.map(key => "dbpedia." + key -> subject.properties(key))
			.toMap

		properties ++= prefixedProperties

		properties.toMap
	}

	def mergeSubjects(subject: Subject, duplicates: List[Subject], version: Version): Subject = {
		val sm = new SubjectManager(subject, version)

		val prefixedProperties = subject
		    .properties
		    .keySet
		    .map(key => "wikidata." + key -> subject.properties(key))
		    .toMap

		val newProperties = duplicates
			.foldLeft(Map[String, List[String]]()) {
				(properties, subject) => properties ++ reduceSubject(subject)
			}

		sm.removeProperties()
		sm.addProperties(prefixedProperties ++ newProperties)
		subject
	}

	def joinSubjectsWithDuplicates(
		subjects: RDD[Subject],
		duplicates: RDD[DuplicateCandidates]
	): RDD[(Subject, List[Subject])] = {
		subjects
			.keyBy(_.id)
		    .join(duplicates.keyBy(_.subject_id))
		    .values
		    .map{
				case (subject, candidates) => (subject, candidates.candidates.map(_._1))
			}
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName(appName)

		val sc = new SparkContext(conf)

		val version = makeTemplateVersion()
		val subjects = sc.cassandraTable[Subject](keyspace, subjectTable)
		val dbpedia = sc.cassandraTable[Subject](keyspace, dbpediaTable)
		val duplicates = sc.cassandraTable[DuplicateCandidates](keyspace, duplicateTable)

		val duplicateIds = duplicates
			.map(_.candidates.map(_._1.id))
		    .reduce(_:::_)
		    .distinct

		val notDuplicates = dbpedia
			.filter(subject => !duplicateIds.contains(subject.id))

		val joinedSubjects = joinSubjectsWithDuplicates(subjects, duplicates)
			.map(x => mergeSubjects(x._1, x._2, version))
		    .union(notDuplicates)

		joinedSubjects.saveToCassandra(keyspace, outputTable)
	}
}
