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

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models.{Subject, Version}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object MasterUpdate extends SparkJob {
	appName = "Master Update"
	configFile = "master_update.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads the Subjects from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = sc.cassandraTable[Subject](settings("subjectKeyspace"), settings("subjectTable"))
		List(subjects).toAnyRDD()
	}

	/**
	  * Writes the updated master nodes to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
	    	.fromAnyRDD[Subject]()
	    	.head
	    	.saveToCassandra(settings("subjectKeyspace"), settings("subjectTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Updates the master nodes by newly generating their data.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val List(subjects) = input.fromAnyRDD[Subject]()
		val version = Version(appName, List("master update"), sc, true, settings.get("subjectTable"))
		val masterGroups = subjects
			.map(subject => (subject.master, List(subject)))
	    	.reduceByKey(_ ++ _)
	    	.values
		val updatesMasters = masterGroups.map { masterGroup =>
			val master = masterGroup.find(_.isMaster).get
			val slaves = masterGroup.filter(_.isSlave)
			Merging.mergeIntoMaster(master, slaves, version).head
		}
		List(updatesMasters).toAnyRDD()
	}
}
