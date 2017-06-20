package de.hpi.ingestion.datamerge

import java.util.UUID
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.models.{Duplicates, Candidate}
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
		val duplicates = sc.cassandraTable[Duplicates](
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
	  * @param score Duplicates object containing the Subject and the staged Subject
	  * @param version Version used for versioning
	  * @return List of the Subject and the staged Subject
	  */
	def addToMasterNode(subject: Subject, staging: Subject, score: Double = 1.0, version: Version): List[Subject] = {
		val subjectManager = new SubjectManager(subject, version)
		val stagingManager = new SubjectManager(staging, version)

		stagingManager.setMaster(subject.id, score)
		subjectManager.addRelations(Map(staging.id -> Map("duplicate" -> score.toString)))
		stagingManager.addRelations(Map(subject.id -> Map("duplicate" -> score.toString)))
		List(subject, staging)
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

	def getPrioritized(key: String, subjects: List[Subject]): List[String] = {
		sourcePriority.flatMap { datasource =>
			subjects
		    	.filter(_.datasource == datasource)
				.flatMap(_.get(key))
		}.distinct
	}

	/**
	  * Merges name, aliases and category of every duplicate and writes them to the master node.
	  * @param masterManager Subject Manager of the master node
	  * @param duplicates List of duplicate Subjects
	  */
	def mergeSimpleAttributes(masterManager: SubjectManager, duplicates: List[Subject]): Unit = {
		val names = getPrioritized("name", duplicates)
		val aliases = names.drop(1) ::: duplicates.flatMap(_.aliases)
		val category = getPrioritized("category", duplicates)

		masterManager.setName(names.headOption)
		masterManager.setAliases(aliases)
		masterManager.setCategory(category.headOption)
	}

	/**
	  * Creates the data of the master node by merging the data of the duplicates.
	  * @param master master node
	  * @param duplicates List of duplicate Subjects used to generate the data
	  * @param version Version used for versioning
	  * @return List of duplicate Subjects and master node containing the merged data
	  */
	def mergeIntoMaster(master: Subject, duplicates: List[Subject], version: Version): List[Subject] = {
		val masterManager = new SubjectManager(master, version)
		mergeSimpleAttributes(masterManager, duplicates)
		masterManager.clearRelations()
		masterManager.addRelations(masterRelations(duplicates))
		masterManager.addRelations(mergeRelations(master, duplicates))
		masterManager.clearProperties()
		masterManager.addProperties(mergeProperties(duplicates))
		master :: duplicates
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
		val duplicates = input(2).asInstanceOf[RDD[Duplicates]]
		val version = Version("Merging", List("merging"), sc, true)

		// TODO merge multiple duplicate candidates with the same subject #430

		val subjectJoinedDuplicates = duplicates
	    	.map(duplicate => (duplicate.subject_id, duplicate))
	    	.join(subjects.map(subject => Tuple2(subject.id, subject)))
	    	.values

		val fullJoinedDuplicates = subjectJoinedDuplicates
			.flatMap { case (duplicate, subject) =>
				duplicate.candidates.map(candidate => (candidate.id, Tuple2(candidate.score, subject)))
			}.rightOuterJoin(stagings.map(staging => Tuple2(staging.id, staging)))
	    	.values

		val taggedSubjects = fullJoinedDuplicates
	    	.flatMap { case (option, staging) => option match {
				case Some((score, subject)) => addToMasterNode(subject, staging, score, version)
				case None =>
					val masterNode = Subject(id = staging.id, datasource = "master")
					addToMasterNode(masterNode, staging, 1.0, version)
			}}.distinct

		val mergedSubjects = taggedSubjects
	    	.map(subject => (subject.id, subject))
	    	.cogroup(subjects.map(subject => Tuple2(subject.id, subject)))
			.values
			.map { case (newDuplicates, oldDuplicates) =>
				val duplicates = newDuplicates ++ oldDuplicates
				val master = duplicates.find(_.datasource == "master").get
				val mergedDuplicates = duplicates.filter(_.datasource != "master").toList
				(master, mergedDuplicates)
			}.flatMap { case (master, duplicates) => mergeIntoMaster(master, duplicates, version) }

		List(mergedSubjects).toAnyRDD()
	}
}
