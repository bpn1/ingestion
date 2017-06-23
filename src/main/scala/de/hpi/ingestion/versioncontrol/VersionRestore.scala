package de.hpi.ingestion.versioncontrol

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import java.util.UUID
import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.framework.SparkJob
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
	  * Writes the restored subjects to a Cassandra table
	  * @param output first element is the RDD of subjects
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
	  * Restores all subjects to the data in the specified Version
	  * @param version TimeUUID of the version that will be reverted to
	  * @return Modified subject with restored data
	  */
	def restoreSubjects(
		subject: Subject,
		version: UUID,
		templateVersion: Version
	): Subject = {
		val sm = new SubjectManager(subject, templateVersion)
		sm.restoreVersion(version)
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
		input
			.fromAnyRDD[Subject]()
			.map(_.map(restoreSubjects(_, version, templateVersion)))
			.toAnyRDD()
	}

	/**
	  * Asserts that two versions are given as program arguments.
	  * @param args arguments of the program
	  * @return true if there are at least two arguments provided
	  */
	override def assertConditions(args: Array[String]): Boolean = {
		args.length == 1
	}
}
