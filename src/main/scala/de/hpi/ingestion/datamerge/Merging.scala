package de.hpi.ingestion.datamerge

import java.util.UUID
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.models.Duplicates
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
	def addToMasterNode(
		subject: Subject,
		staging: Subject,
		score: Double,
		version: Version
	): List[(Subject, Map[UUID, Map[String, String]])] = {
		val stagingManager = new SubjectManager(staging, version)
		stagingManager.setMaster(subject.master, score)

		List(
			(subject, Map(staging.id -> SubjectManager.isDuplicateRelation(score))),
			(staging, Map(subject.id -> SubjectManager.isDuplicateRelation(score)))
		)
	}

	/**
	  * Adds the a master node for a staging Subject
	  * @param staging staging of the duplicate pair
	  * @param version Version used for versioning
	  * @return List of the subject, staging and master Subject
	  */
	def addToMasterNode(staging: Subject, version: Version): List[(Subject, Map[UUID, Map[String, String]])] = {
		val master = Subject.master()
		val stagingManager = new SubjectManager(staging, version)
		stagingManager.setMaster(master.master)

		List(
			(master, Map[UUID, Map[String, String]]()),
			(staging, Map[UUID, Map[String, String]]())
		)
	}

	/**
	  * Merges the properties of the duplicates and uses the non empty properties of the best data source defined by
	  * {@sourcePriority}.
	  * @param slaves List of slave Subjects
	  * @return Map containing the properties and the values
	  */
	def mergeProperties(slaves: List[Subject]): Map[String, List[String]] = {
		Subject
			.normalizedPropertyKeys
			.map(property => property -> getPrioritizedSingle(property, slaves))
			.filter(_._2.nonEmpty)
			.toMap
	}

	/**
	  * Creates master slave relations to every duplicate using the score of the duplicates slave master relation.
	  * @param slaves List of slave Subjects to which the relations will point
	  * @return Map of every duplicates UUID to the relation properties containing the score
	  */
	def masterRelations(slaves: List[Subject]): Map[UUID, Map[String, String]] = {
		slaves
			.map(subject => subject.id -> SubjectManager.masterRelation(subject.masterScore.get))
			.toMap
	}

	/**
	  * Merges the relations of the duplicates and uses the non empty relation property of the best data source defined
	  * by {@sourcePriority}.
	  * @param slaves List of slave Subject
	  * @return Map containing the relations and their properties
	  */
	def mergeRelations(slaves: List[Subject]): Map[UUID, Map[String, String]] = {
		sourcePriority
			.reverse
			.flatMap { datasource =>
				slaves
					.filter(_.datasource == datasource)
					.map(_.masterRelations)
			}.foldLeft(Map[UUID, Map[String, String]]()) { (accum, relations) =>
				(accum.keySet ++ relations.keySet)
					.map(key => key -> (accum.getOrElse(key, Map()) ++ relations.getOrElse(key, Map())))
					.toMap
			}.filter(_._2.nonEmpty)
	}

	/**
	  * Retrieves Attributes regarding the source priorities
	  * @param key name of the attribute
	  * @param slaves subjects from which the attribute should be retrieved from
	  * @return values of the attribute
	  */
	def getPrioritized(key: String, slaves: List[Subject]): List[String] = {
		sourcePriority.flatMap { datasource =>
			slaves
				.filter(_.datasource == datasource)
				.flatMap(_.get(key))
		}.distinct
	}

	/**
	  * Retrieve Attributes regarding the source priorities taking the first non empty candidate
	  * @param key name of the attribute
	  * @param slaves subjects from which the attribute should be retrieved from
	  * @return values of the attribute
	  */
	def getPrioritizedSingle(key: String, slaves: List[Subject]): List[String] = {
		sourcePriority.map { datasource =>
			slaves
				.filter(_.datasource == datasource)
				.flatMap(_.get(key))
		}.find(_.nonEmpty)
		.getOrElse(Nil)
	}

	/**
	  * Merges name, aliases, category and properties of every duplicate and writes them to the master node.
	  * @param masterManager Subject Manager of the master node
	  * @param slaves List of slaves
	  */
	def mergeAttributes(masterManager: SubjectManager, slaves: List[Subject]): Unit = {
		val names = getPrioritized("name", slaves)
		val aliases = names.drop(1) ::: getPrioritized("aliases", slaves)
		val category = getPrioritizedSingle("category", slaves)
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
	  * @param slaves List of slaves
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
	  * @param slaves List of slave Subjects used to generate the data
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
		val version = Version("Merging", List("merging"), sc, true, settings.get("subjectTable"))

		// TODO merge multiple duplicate candidates with the same subject #430

		val subjectIdTuple = subjects.map(subject => (subject.id, subject))
		val subjectMasterTuple = subjects.map(subject => (subject.master, subject))
		val stagingTuple = stagings.map(staging => (staging.id, staging))

		val subjectJoinedDuplicates : RDD[(Duplicates, Subject)] = duplicates
			.keyBy(_.subject_id)
			.join(subjectIdTuple)
			.values

		val stagingJoinedDuplicates : RDD[(Option[(Double, Subject)], Subject)] = subjectJoinedDuplicates
			.flatMap { case (duplicate, subject) =>
				duplicate.candidates.map(candidate => (candidate.id, Tuple2(candidate.score, subject)))
			}.rightOuterJoin(stagingTuple)
			.values

		val taggedSubjects : RDD[Subject] = stagingJoinedDuplicates
			.flatMap {
				case (Some((score, subject)), staging) => addToMasterNode(subject, staging, score, version)
				case (None, staging) => addToMasterNode(staging, version)
			}.reduceByKey(_ ++ _)
	    	.map {
				case (subject, relations) if relations.nonEmpty =>
					val subjectManager = new SubjectManager(subject, version)
					subjectManager.addRelations(relations)
					subject
				case (subject, _) => subject
			}

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
