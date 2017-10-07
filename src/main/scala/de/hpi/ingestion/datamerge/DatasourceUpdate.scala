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

package de.hpi.ingestion.datamerge

import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Updates the current Subjects with the data of an already integrated datasource. The new data is added to the
  * already existing Subjects representing the same entity of the given datasource.
  */
object DatasourceUpdate extends SparkJob {
	appName = "Datasource Update"
	configFile = "datasource_update.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads the new Subjects used to update the current Subjects and the current Subjects.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val newSubjects = sc.cassandraTable[Subject](settings("inputKeyspace"), settings("inputTable"))
		val subjects = sc.cassandraTable[Subject](settings("outputKeyspace"), settings("outputTable"))
		List(newSubjects, subjects).toAnyRDD()
	}

	/**
	  * Saves the updated and new Subjects to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
	    	.fromAnyRDD[Subject]()
	    	.head
	    	.saveToCassandra(settings("outputKeyspace"), settings("outputTable"))
	}
	// $COVERAGE-ON$
	/**
	  * Updates the old Subject with the data of the new Subject.
	  * @param oldSubject old Subject to be updated
	  * @param newSubject Subject containing the new data with which the old Subject is updated
	  * @param version Version used for versioning
	  * @return the updated Subject
	  */
	def updateSubject(oldSubject: Subject, newSubject: Subject, version: Version): Subject = {
		val sm = new SubjectManager(oldSubject, version)
		sm.setName(newSubject.name)
		sm.addAliases(newSubject.aliases)
		sm.setCategory(newSubject.category)
		sm.overwriteProperties(newSubject.properties)
		sm.overwriteRelations(newSubject.relations)
		oldSubject
	}

	/**
	  * Updates the current Subjects with new Subjects of an already integrated datasource.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val List(newSubjects, subjects) = input.fromAnyRDD[Subject]()
		val version = Version(appName, List("datasourceupdate"), sc, false, settings.get("inputTable"))
		val updateAbleSubjects = subjects
			.filter(_.isSlave)
			.flatMap { subject =>
				val keys = subject.get(settings("datasourceKey"))
				keys.map(key => (key, subject))
			}
		val updatedSubjects = newSubjects
	    	.flatMap { subject =>
				val keys = subject.get(settings("datasourceKey"))
				keys.map(key => (key, subject))
			}.leftOuterJoin(updateAbleSubjects)
			.values
			.flatMap {
				case (newSubject, Some(oldSubject)) => List(updateSubject(oldSubject, newSubject, version))
				case (newSubject, None) => Merging.addToMasterNode(newSubject, version).map(_._1)
			}
		List(updatedSubjects).toAnyRDD()
	}
}
