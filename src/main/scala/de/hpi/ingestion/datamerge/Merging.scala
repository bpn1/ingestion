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
import de.hpi.ingestion.implicits.TupleImplicits._

/**
  * Merging-Job for merging duplicates into the subject table
  */
object Merging extends SparkJob {
	appName = "Merging"
	configFile = "merging.xml"
	val sourcePriority = List("human", "implisense", "dbpedia", "wikidata")

	// $COVERAGE-OFF$
	/**
	  * Loads the Subjects, the staged Subjects and the Duplicate Candidates from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = sc.cassandraTable[Subject](settings("keyspaceSubjectTable"), settings("subjectTable"))
		val stagings = sc.cassandraTable[Subject](settings("keyspaceStagingTable"), settings("stagingTable"))
		val duplicates = sc.cassandraTable[DuplicateCandidates](
			settings("keyspaceDuplicatesTable"),
			settings("duplicatesTable"))
		List(subjects, stagings).toAnyRDD() ::: List(duplicates).toAnyRDD()
	}

	/**
	  * Saves the merged Subjects to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[Subject]()
			.head
			.saveToCassandra(settings("keyspaceSubjectTable"), settings("subjectTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Sets the master node for the staged subject and adds symmetrical duplicate relation between the Subject and the
	  * staged Subject if the Subject is not a master node.
	  * @param subject existing Subject
	  * @param staged duplicate Subject
	  * @param duplicateCandidates Duplicate Candidates object containing the Subject and the staged Subject
	  * @param version Version used for versioning
	  * @return List of the Subject and the staged Subject
	  */
	def addToMasterNode(
		subject: Subject,
		staged: Subject,
		duplicateCandidates: Option[DuplicateCandidates],
		version: Version
	): List[Subject] = {
		val score = duplicateCandidates
			.flatMap(_.candidates.find(_._1 == staged.id))
			.map(_._3)
			.getOrElse(1.0)
		val stagedSm = new SubjectManager(staged, version)
		val subjectSm = new SubjectManager(subject, version)
		subject.master match {
			case Some(masterId) =>
				duplicateCandidates.foreach { dupCandidate =>
					dupCandidate.candidates.foreach { case (duplicate, source, dupeScore) =>
						subjectSm.addRelations(Map(duplicate -> Map("duplicate" -> dupeScore.toString)))
					}
				}
				stagedSm.addRelations(Map(subject.id -> Map("duplicate" -> score.toString)))
				stagedSm.setMaster(masterId, score)
			case None => stagedSm.setMaster(subject.id, score)
		}
		List(subject, staged)
	}

	/**
	  * Merges the properties of the duplicates and uses the non empty properties of the best data source defined by
	  * {@sourcePriority}.
	  * @param duplicates List of duplicate Subjects
	  * @return Map containing the properties and the values
	  */
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

	/**
	  * Merges the relations of the duplicates and uses the non empty relation property of the best data source defined
	  * by {@sourcePriority}.
	  * @param master master node of the duplicates
	  * @param duplicates List of duplicate Subject
	  * @return Map containing the relations and their properties
	  */
	def mergeRelations(master: Subject, duplicates: List[Subject]): Map[UUID, Map[String, String]] = {
		val rel = duplicates
			.flatMap(_.relations.keySet)
			.filter(_ != master.id)
			.map { relationSub =>
				val relation = sourcePriority.map { dataSource =>
					duplicates.flatMap { duplicate =>
						duplicate.relations_history
							.getOrElse(relationSub, Map())
							.filter(_._2.lastOption.exists(_.datasources.contains(dataSource)))
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

	/**
	  * Creates the data of the master node by merging the data of the duplicates.
	  * @param master master node
	  * @param duplicates List of duplicate Subjects used to generate the data
	  * @param version Version used for versioning
	  * @return List of duplicate Subjects and master node containing the merged data
	  */
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

	/**
	  * Merges the staged Subjects into the current Subjects using the Duplicate Candidates.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val List(subjects, stagings) = input.take(2).fromAnyRDD[Subject]()
		val duplicateCandidates = input(2).asInstanceOf[RDD[DuplicateCandidates]]
		val version = Version("Merging", List("merging"), sc, true)

		// TODO merge multiple duplicate candidates with the same subject #430
		val mergedSubjects = duplicateCandidates
			.map(candidate => (candidate.subject_id, candidate))
			.join(subjects.map(sub => Tuple2(sub.id, sub)))
			.values
			.flatMap { case (candidate, sub) =>
				candidate.candidates.map(tuple => (tuple._1, (candidate, sub)))
			}.rightOuterJoin(stagings.map(sub => Tuple2(sub.id, sub)))
			.values
			.flatMap { case (candidateTupleOption, staged) =>
				val (candidateOpt, subjectOpt) = candidateTupleOption.unzip.map(_.headOption, _.headOption)
				addToMasterNode(subjectOpt.getOrElse(Subject()), staged, candidateOpt, version)
			}.distinct
			.map(sub => (sub.master.getOrElse(sub.id), sub))
			.cogroup(subjects.map(sub => Tuple2(sub.master.getOrElse(sub.id), sub)))
			.map { case (masterId, (duplicates, existingMasterAndDuplicates)) =>
				val master = existingMasterAndDuplicates
					.find(_.id == masterId)
					.orElse(duplicates.find(_.id == masterId))
					.get
				val mergedDuplicates = (duplicates ++ existingMasterAndDuplicates)
					.filter(_.id != masterId)
					.toList
				(master, mergedDuplicates)
			}.flatMap { case (master, dupes) => mergeIntoMaster(master, dupes, version)}
		List(mergedSubjects).toAnyRDD()
	}
}
