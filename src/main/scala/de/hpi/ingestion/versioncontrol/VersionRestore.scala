package de.hpi.ingestion.versioncontrol

import de.hpi.ingestion.datalake.models._
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import java.util.UUID

import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.framework.SparkJob
import play.api.libs.json._

import scala.collection.mutable
import de.hpi.ingestion.implicits.TupleImplicits._
import de.hpi.ingestion.versioncontrol.models.HistoryEntry
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._

/**
  * Resets all subject data (normal attributes, properties entries and relations) to the last version
  * that has a timestamp BEFORE the argument timestamp OR is equal to the argument TimeUUID
  */
object VersionRestore extends SparkJob {
	appName = "VersionRestore"
	val keyspace = "datalake"
	val tablename = "subject"
	val outputTablename = "subject_restored"
	val NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L

	// $COVERAGE-OFF$
	/**
	  * Loads subjects from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = sc.cassandraTable[Subject](keyspace, tablename)
		List(subjects.asInstanceOf[RDD[Any]])
	}

	/**
	  * Writes the JSON diff to the HDFS.
	  * @param output first element is the RDD of JSON diffs
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[Subject]()
			.head
			.saveToCassandra(keyspace, outputTablename)
	}
	// $COVERAGE-ON$

	/**
	  * Extracts the time component of a TimeUUID.
	  * Source: https://git.io/vSxjU
	  * @param uuid TimeUUID used to extract the time
	  * @return time as long
	  */
	def timeFromUUID(uuid: UUID): Long = (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000

	/**
	  * Returns list of values of the given version or a previous version that changed the given field.
	  * @param queryVersionID TimeUUID of the wanted version of the attributes
	  * @param versionList list of all existing versions of the field
	  * @return value of the field at the point of the queried version
	  */
	// TODO filter by timestamp, not exact match
	def findVersion(queryVersionID: UUID, versionList: List[Version]): Option[Version] = {
		val queryVersion = versionList.find(_.version == queryVersion) //.map(_.value)
		val olderVersions = versionList.filter(v => timeFromUUID(v.version) < timeFromUUID(queryVersionID))
		val olderVersion = if(olderVersions.nonEmpty) {
			val lastVersion = olderVersions.maxBy(v => timeFromUUID(v.version))
			Option(lastVersion)
		} else {
			None
		}
		queryVersion.orElse(olderVersion)
	}

	/**
	  * Returns tuple of old and new values for the given field.
	  * @param version TimeUUID of the version that will be reverted to
	  * @param versionList list of all existing versions of the field
	  * @return Option of tuple of old and new values
	  */
	def createValueList(
		version: UUID,
		versionList: List[Version]
	): Option[List[String]] = {
		findVersion(version, versionList) match {
			case Nil => None
			case t => Option(t)
		}
	}

	/**
	  * Transforms version histories to the data in that version
	  * @param subject Subject containing the data
	  * @param version TimeUUID of the version that will be reverted to
	  * @return Modified subject with restored data
	  */
	def restoreSubjects(
		subject: Subject,
		version: UUID,
		templateVersion: Version
	): Subject = {
		val name = createValueList(version, subject.name_history).getOrElse(List()).headOption
		val aliases = createValueList(version, subject.aliases_history).getOrElse(List())
		val category = createValueList(version, subject.category_history).getOrElse(List()).headOption
		val properties = subject.properties_history.mapValues(createValueList(version, _).getOrElse(List()))
		val relations = subject.relations_history
			.mapValues(_.mapValues(createValueList(version, _).getOrElse(List("")).head))

		// TODO add data sources from specific attribute to that attribute's version (or concat them all)
		val sm = new SubjectManager(subject, templateVersion)

		// write fields into subject if found
		sm.setName(name)
		sm.setAliases(aliases)
		sm.setCategory(category)
		sm.setProperties(properties)
		sm.setRelations(relations)

		subject
	}

	/**
	  * Creates a diff for each subject containing the deletions and additions of every field between the two versions.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val version = UUID.fromString(args(0))
		val templateVersion = Version(appName, List("history"), sc, timestampName = false)
		input.fromAnyRDD[Subject]()
			.map(rdd => rdd.map(restoreSubjects(_, version, templateVersion)))
			.toAnyRDD()
	}

	/**
	  * Asserts that two versions are given as program arguments.
	  * @param args arguments of the program
	  * @return true if there are at least two arguments provided
	  */
	override def assertConditions(args: Array[String]): Boolean = {
		args.length >= 1
	}
}
