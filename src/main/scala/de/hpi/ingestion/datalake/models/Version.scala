package de.hpi.ingestion.datalake.models

import java.util.{Date, UUID}

import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector._
import org.apache.spark.SparkContext

/**
  * Case class representing a Version used for the versioning of every Subjects data during a single job.
  * @param version TimeUUID of the Version used for bookkeeping
  * @param program name of the program writing the version
  * @param value value of the data field that is versioned with this version
  * @param validity Map containing the data about the temporal validity of the data
  * @param datasources datasources used in the job that created this version
  * @param timestamp timestamp of the creation of this version
  */
case class Version(
	version: UUID = UUIDs.timeBased(),
	var program: String,
	var value: List[String] = Nil,
	var validity: Map[String, String] = Map[String, String](),
	var datasources: List[String] = Nil,
	var timestamp: Date = new Date())

/**
  * Companion object for the Version case class.
  */
object Version {
	val keyspace = "datalake"
	val tablename = "version"

	// $COVERAGE-OFF$
	/**
	  * Writes Version to the version table in Cassandra.
	  * @param version Version to write
	  * @param sc Spark Context used to connect to the Cassandra.
	  */
	def writeToCassandra(version: Version, sc: SparkContext): Unit = {
		if(sc.getConf.contains("spark.cassandra.connection.host")) {
			val logVersion = (version.version, version.timestamp, version.datasources, version.program)
			sc.parallelize(List(logVersion))
				.saveToCassandra(keyspace, tablename, SomeColumns("version", "timestamp", "datasources", "program"))
		}
	}
	// $COVERAGE-ON$

	/**
	  * Creates Version with the given program and datasources and timestamps of the current time. Also writes the
	  * Version to the Cassandra if the Spark Context has a Cassandra connection.
	  * @param program name of the program writing the version
	  * @param datasources List of datasources used
	  * @param sc Spark Context used to connect to the Cassandra
	  * @param timestampName whether or not a timestamp will be appended to the program name
	  * @return Version with the given parameters
	  */
	def apply(program: String, datasources: List[String], sc: SparkContext, timestampName: Boolean): Version = {
		val time = new Date()
		val timestamp = s"_$time".filter(c => timestampName)
		val version = Version(program = s"$program$timestamp", datasources = datasources, timestamp = time)
		writeToCassandra(version, sc)
		version
	}
}
