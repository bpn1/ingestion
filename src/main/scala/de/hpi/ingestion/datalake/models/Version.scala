/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	var validity: Map[String, String] = Map(),
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
	def writeToCassandra(version: Version, sc: SparkContext, subjecttable: Option[String]): Unit = {
		if(sc.getConf.contains("spark.cassandra.connection.host")) {
			val logVersion = (version.version, version.timestamp, version.datasources, version.program, subjecttable)
			sc.parallelize(List(logVersion)).saveToCassandra(keyspace, tablename,
				SomeColumns("version", "timestamp", "datasources", "program", "subjecttable"))
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
	  * @param subjecttable Name of the table that this Version's changes will be written to
	  * @return Version with the given parameters
	  */
	def apply(
		program: String,
		datasources: List[String],
		sc: SparkContext,
		timestampName: Boolean,
		subjecttable: Option[String]
	): Version = {
		val time = new Date()
		val timestamp = s"_$time".filter(c => timestampName)
		val version = Version(program = s"$program$timestamp", datasources = datasources, timestamp = time)
		writeToCassandra(version, sc, subjecttable)
		version
	}
}
