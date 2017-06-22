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
	  * Adds the duplicates relation and a master node for a duplicate pair
	  * @param subject subject of the duplicate pair
	  * @param staging staging of the duplicate pair
	  * @param score Duplicates object containing the Subject and the staged Subject
	  * @param version Version used for versioning
	  * @return List of the subject, staging and master Subject
	  */
	def addToMasterNode(subject: Subject, staging: Subject, score: Double, version: Version): List[Subject] = {
		val subjectManager = new SubjectManager(subject, version)
		val stagingManager = new SubjectManager(staging, version)

		stagingManager.setMaster(subject.master, score)
		subjectManager.addRelations(Map(staging.id -> Map("duplicate" -> score.toString)))
		stagingManager.addRelations(Map(subject.id -> Map("duplicate" -> score.toString)))
		List(subject, staging)
	}

	/**
	  * Adds the a master node for a staging Subject
	  * @param staging staging of the duplicate pair
	  * @param version Version used for versioning
	  * @return List of the subject, staging and master Subject
	  */
	def addToMasterNode(staging: Subject, version: Version): List[Subject] = {
		val master = Subject.master()
		val stagingManager = new SubjectManager(staging, version)

		stagingManager.setMaster(master.master)
		List(staging, master)
	}

	/**
	  * Merges the properties of the duplicates and uses the non empty properties of the best data source defined by
	  * {@sourcePriority}.
	  * @param duplicates List of duplicate Subjects
	  * @return Map containing the properties and the values
	  */
	def mergeProperties(duplicates: List[Subject]): Map[String, List[String]] = {
		Subject
	    	.normalizedPropertyKeys
	    	.map(property => property -> getPrioritized(property, duplicates))
	    	.filter(_._2.nonEmpty)
	    	.toMap
	}

	/**
	  * Creates master slave relations to every duplicate using the score of the duplicates slave master relation.
	  * @param duplicates List of duplicate Subjects to which the relations will point
	  * @return Map of every duplicates UUID to the relation properties containing the score
	  */
	def masterRelations(duplicates: List[Subject]): Map[UUID, Map[String, String]] = {
		duplicates
			.map(duplicate => duplicate.id -> SubjectManager.masterRelation(duplicate.masterScore))
	    	.toMap
	}

	/**
	  * Merges the relations of the duplicates and uses the non empty relation property of the best data source defined
	  * by {@sourcePriority}.
	  * @param duplicates List of duplicate Subject
	  * @return Map containing the relations and their properties
	  */
	def mergeRelations(duplicates: List[Subject]): Map[UUID, Map[String, String]] = {
		sourcePriority
			.reverse
			.flatMap { datasource =>
				duplicates
					.filter(_.datasource == datasource)
					.map(_.relations)
			}.foldLeft(Map[UUID, Map[String, String]]()) { (accum, relations) =>
				val keys = accum.keySet ++ relations.keySet
				keys
					.map(key => key -> (accum.getOrElse(key, Map()) ++ relations.getOrElse(key, Map())))
					.toMap
			}
	}

	/**
	  * Retrieves Attributes
	  * @param key
	  * @param subjects
	  * @return
	  */
	def getPrioritized(key: String, subjects: List[Subject]): List[String] = {
		sourcePriority.flatMap { datasource =>
			subjects
		    	.filter(_.datasource == datasource)
				.flatMap(_.get(key))
		}.distinct
	}

	/**
	  * Merges name, aliases, category and properties of every duplicate and writes them to the master node.
	  * @param masterManager Subject Manager of the master node
	  * @param slaves List of duplicates
	  */
	def mergeAttributes(masterManager: SubjectManager, slaves: List[Subject]): Unit = {
		val names = getPrioritized("name", slaves)
		val aliases = names.drop(1) ::: slaves.flatMap(_.aliases)
		val category = getPrioritized("category", slaves)
		val properties = mergeProperties(slaves)

		masterManager.setName(names.headOption)
		masterManager.setAliases(aliases)
		masterManager.setCategory(category.headOption)

		masterManager.clearProperties()
		masterManager.addProperties(properties)
	}

	/**
	  * Merges relations of every duplicate and writes them to the master node.
	  * @param masterManager Subject Manager of the master node
	  * @param slaves List of duplicates
	  */
	def mergeRelations(masterManager: SubjectManager, slaves: List[Subject]): Unit = {
		val masterRelations = this.masterRelations(slaves)
		val mergedRelations = this.mergeRelations(slaves)

		masterManager.clearRelations()
		masterManager.addRelations(masterRelations)
		masterManager.addRelations(mergedRelations)
	}

	/**
	  * Creates the data of the master node by merging the data of the duplicates.
	  * @param master master node
	  * @param slaves List of duplicate Subjects used to generate the data
	  * @param version Version used for versioning
	  * @return List of duplicate Subjects and master node containing the merged data
	  */
	def mergeIntoMaster(master: Subject, slaves: List[Subject], version: Version): List[Subject] = {
		val masterManager = new SubjectManager(master, version)
		mergeAttributes(masterManager, slaves)
		mergeRelations(masterManager, slaves)
		master :: slaves
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

		val subjectIdTuple = subjects.map(subject => (subject.id, subject))
		val subjectMasterTuple = subjects.map(subject => (subject.master, subject))
		val stagingTuple = stagings.map(staging => (staging.id, staging))

		val subjectJoinedDuplicates : RDD[(Duplicates, Subject)] = duplicates
	    	.map(duplicate => (duplicate.subject_id, duplicate))
	    	.join(subjectIdTuple)
	    	.values

		val stagingJoinedDuplicates : RDD[(Option[(Double, Subject)], Subject)] = subjectJoinedDuplicates
			.flatMap { case (duplicate, subject) =>
				duplicate.candidates.map(candidate => (candidate.id, Tuple2(candidate.score, subject)))
			}.rightOuterJoin(stagingTuple)
	    	.values

		val taggedSubjects : RDD[Subject] = stagingJoinedDuplicates
	    	.flatMap { case (option, staging) => option match {
				case Some((score, subject)) => addToMasterNode(subject, staging, score, version)
				case None => addToMasterNode(staging, version)
			}}.distinct

		val mergedSubjects = taggedSubjects
	    	.map(subject => (subject.master, subject))
	    	.cogroup(subjectMasterTuple)
			.map { case (id, (newDuplicates, oldDuplicates)) =>
				val duplicates = (newDuplicates ++ oldDuplicates).toList.distinct
				val (master, slaves) = duplicates.partition(_.id == id)
				(master.head, slaves)
			}.flatMap { case (master, slaves) => mergeIntoMaster(master, slaves, version) }

		List(mergedSubjects).toAnyRDD()
	}
}
