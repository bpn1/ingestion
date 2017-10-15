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

package de.hpi.ingestion.versioncontrol

import java.util.UUID

import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Resets all subject data (normal attributes, properties entries and relations) to the last version
  * that has a timestamp BEFORE the argument timestamp OR is equal to the argument TimeUUID
  */
class VersionRestore extends SparkJob {
	import VersionRestore._
	appName = "VersionRestore"
	val keyspace = "datalake"
	val tablename = "subject"
	val outputTablename = "subject_restored"

	var subjects: RDD[Subject] = _
	var restoredSubjects: RDD[Subject] = _

	// $COVERAGE-OFF$
	/**
	  * Loads subjects from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext): Unit = {
		subjects = sc.cassandraTable[Subject](keyspace, tablename)
	}

	/**
	  * Writes the restored subjects to a Cassandra table
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		restoredSubjects.saveToCassandra(keyspace, outputTablename)
	}
	// $COVERAGE-ON$

	/**
	  * Creates a diff for each subject containing the deletions and additions of every field between the two versions.
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @return List of RDDs containing the output data
	  */
	override def run(sc: SparkContext): Unit = {
		val restoreVersion = UUID.fromString(conf.restoreVersion)
		val version = Version(appName, List("history"), sc, false, Option(outputTablename))
		restoredSubjects = subjects.map(restoreSubjects(_, restoreVersion, version))
	}

	/**
	  * Asserts that two versions are given as program arguments.
	  * @return true if there are at least two arguments provided
	  */
	override def assertConditions(): Boolean = conf.restoreVersionOpt.isDefined
}

object VersionRestore {
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
}
