package de.hpi.ingestion.datamerge

import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.models.DuplicateCandidates
import de.hpi.ingestion.implicits.CollectionImplicits._

/**
  * Merging-Job for merging duplicates into the subject table
  */
object Merging extends SparkJob {
	appName = "Merging"
	val keyspace = "datalake"
	val subjectsTable = "subject"
	val stagingsTable = "subject_temp"
	val duplicatesTable = "duplicatecandidates"
	val outputTable = "subject"

	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = sc.cassandraTable(keyspace, subjectsTable)
		val stagings = sc.cassandraTable(keyspace, stagingsTable)
		val duplicates = sc.cassandraTable(keyspace, duplicatesTable)
		List(subjects, stagings).toAnyRDD() ::: List(duplicates).toAnyRDD()
	}

	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[Subject]()
			.head
			.saveToCassandra(keyspace, outputTable)
	}

	/**
	  * Add symmetric relations between two subject nodes, abstraction from SubjectManagers
	  * addRelations method
	  * @param subject1 UUID of a given node
	  * @param subject2 UUID of another given node
	  * @param relations A set of attributes of keys and values describing the relation,
	  * typically a 'type' value should be passed
	  * @param version Current transactional id handle
	  */
	def addSymmetricRelation(
		subject1: Subject,
		subject2: Subject,
		relations: Map[String,String],
		version: Version
	): Unit = {
		val subject_manager1 = new SubjectManager(subject1, version)
		val subject_manager2 = new SubjectManager(subject2, version)

		subject_manager1.addRelations(Map(subject2.id -> relations))
		subject_manager2.addRelations(Map(subject1.id -> relations))
	}

	def mergeInformation(master: Subject, duplicates: DuplicateCandidates, version: Version): Subject = {
		val master_manager = new SubjectManager(master, version)
		val mergedProperties = duplicates.candidates.map { case (duplicate, source, score) =>
			duplicate.toProperties(source)
		}.reduceLeft((properties, duplicateProperties) => {
			// Merging two property maps together
			(properties.toSeq ++ duplicateProperties.toSeq)
				.groupBy(_._1)
				.mapValues(_.reduce(_._2 ::: _._2))
		})
		master_manager.addProperties(mergedProperties)
		master
	}

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val inputSubjects = input.fromAnyRDD[Subject]()
		val subject = inputSubjects.head
		val stagings = inputSubjects(1)
		val inputDuplicates = input.fromAnyRDD[DuplicateCandidates]()
		val duplicates = inputDuplicates.last

		val version = Version(program = "Merging")

		val joined = subject
			.keyBy(_.id)
			.join(duplicates.keyBy(_.subject_id))
			.values

		val mergedDuplicates = joined.flatMap { case (master, candidates) =>
			val duplicates = candidates.candidates.map { case (duplicate, source, score) =>
				val relation = Map("type" -> "isDuplicate", "confidence" -> score.toString)
				addSymmetricRelation(master, duplicate, relation, version)
				duplicate
			}
			mergeInformation(master, candidates, version) :: duplicates
		}.distinct

		val nonDuplicates = stagings
			.keyBy(_.id)
			.subtractByKey(duplicates.flatMap { candidates =>
				candidates.candidates.map(_._1)
			}.keyBy(_.id))
			.values

		List(mergedDuplicates.union(nonDuplicates)).toAnyRDD()
	}
}
