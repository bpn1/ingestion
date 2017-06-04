package de.hpi.ingestion.datamerge

import java.util.UUID

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
	val sourcePriority = List("human", "implisense", "dbpedia", "wikidata")

	// $COVERAGE-OFF$
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
	// $COVERAGE-ON$

	def addToMasterNode(subject: Subject, staged: Subject, score: Double, version: Version): List[Subject] = {
		val masterId = subject.master.getOrElse(subject.id)
		val stagedSm = new SubjectManager(staged, version)
		stagedSm.setMaster(masterId, score)
		List(subject, staged)
	}

	def mergeProperties(duplicates: List[Subject]): Map[String, List[String]] = {
		Subject.normalizedPropertyKeys.flatMap { prop =>
			val propValues = sourcePriority.map { dataSource =>
				duplicates.flatMap { duplicate =>
					duplicate.properties_history
						.get(prop)
						.map(_.last)
				}.filter(_.datasources.contains(dataSource))
				.map(_.value)
				.fold(List.empty[String])(_ ++ _)
				.distinct
			}.find(_.nonEmpty)
			propValues.map((prop, _))
		}.filter(_._2.nonEmpty)
		.toMap
	}

	/**
	  * Creates master slave relations to every duplicate using the score of the duplicates slave master relation.
	  * @param duplicates List of duplicate Subjects to which the relations will point
	  * @return Map of every duplicates UUID to the relation properties containing the score
	  */
	def masterRelations(duplicates: List[Subject]): Map[UUID, Map[String, String]] = {
		duplicates.flatMap { duplicate =>
			duplicate.masterScore()
				.map(SubjectManager.masterRelation)
				.map((duplicate.id, _))
		}.toMap
	}

	def mergeRelations(master: Subject, duplicates: List[Subject]): Map[UUID, Map[String, String]] = {
		val rel = duplicates
			.flatMap(_.relations.keySet)
			.filter(_ != master.id)
			.map { relationSub =>
				val relation = sourcePriority.map { dataSource =>
					duplicates.flatMap { duplicate =>
						duplicate.relations_history
							.getOrElse(relationSub, Map())
							.filter(_._2.last.datasources.contains(dataSource))
							.mapValues(_.last.value.headOption)
							.collect { case (prop, Some(value)) => (prop, value) }
					}.toMap
				}.fold(Map()) { (cumulativeMap, dataSourceMap) =>
					cumulativeMap ++ (dataSourceMap -- cumulativeMap.keySet)
				}
				(relationSub, relation)
			}.toMap
		rel
	}

	/**
	  * Merges name, aliases and category of every duplicate and writes them to the master node.
	  * @param masterSM Subject Manager of the master node
	  * @param duplicates List of duplicate Subjects
	  */
	def mergeSimpleAttributes(masterSM: SubjectManager, duplicates: List[Subject]): Unit = {
		val names = sourcePriority.flatMap { dataSource =>
			duplicates
				.filter(_.name_history.lastOption.exists(_.datasources.contains(dataSource)))
				.flatMap(_.name)
		}.distinct
		val aliases = names.drop(1) ++ duplicates.flatMap(_.aliases)
		val category = sourcePriority.flatMap { dataSource =>
			duplicates
				.filter(_.category_history.lastOption.exists(_.datasources.contains(dataSource)))
				.flatMap(_.category)
		}.headOption
		masterSM.setName(names.headOption)
		masterSM.setAliases(aliases)
		masterSM.setCategory(category)
	}

	def mergeIntoMaster(master: Subject, duplicates: List[Subject], version: Version): List[Subject] = {
		val masterSM = new SubjectManager(master, version)
		mergeSimpleAttributes(masterSM, duplicates)
		masterSM.clearRelations()
		masterSM.addRelations(masterRelations(duplicates))
		masterSM.addRelations(mergeRelations(master, duplicates))
		masterSM.clearProperties()
		masterSM.addProperties(mergeProperties(duplicates))
		master +: duplicates
	}

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val List(subjects, stagings) = input.take(2).fromAnyRDD[Subject]()
		val duplicateCandidates = input(2).asInstanceOf[RDD[DuplicateCandidates]]
		val version = Version("Merging", Nil, sc, true)

		val mergedSubjects = duplicateCandidates
			.map(candidate => (candidate.subject_id, candidate))
			.join(subjects.map(sub => Tuple2(sub.id, sub)))
			.values
			.flatMap { case (candidate, sub) =>
				candidate.candidates.map(tuple => (tuple._1, (candidate, sub)))
			}.rightOuterJoin(stagings.map(sub => Tuple2(sub.id, sub)))
			.values
			.flatMap {
				case (Some((candidate, sub)), staged) =>
					val score = candidate.candidates.find(_._1 == staged.id).get._3
					addToMasterNode(sub, staged, score, version)
				case (None, staged) => addToMasterNode(Subject(), staged, 1.0, version)
			}.distinct
			.map(sub => sub.master match {
				case Some(id) => (id, List(sub))
				case None => (sub.id, Nil)
			}).reduceByKey(_ ++ _)
			.leftOuterJoin(subjects.map(sub => Tuple2(sub.id, sub)))
			.map {
				case (masterId, (duplicates, Some(master))) =>
					(master, duplicates)
				case (masterId, (duplicates, None)) =>
					val master = duplicates.find(_.id == masterId).get
					(master, duplicates.filter(_.id != masterId))
			}.flatMap { case (master, dupes) => mergeIntoMaster(master, dupes, version)}
		List(mergedSubjects).toAnyRDD()
	}
}
