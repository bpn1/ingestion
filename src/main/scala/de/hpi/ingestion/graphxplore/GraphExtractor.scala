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

package de.hpi.ingestion.graphxplore

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models.Subject
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.graphxplore.models.ResultGraph
import de.hpi.ingestion.implicits.CollectionImplicits._

/**
  * Base trait for graph extraction jobs; converts the subject table into a GraphFrames instance
  */
trait GraphExtractor extends SparkJob {
	appName = "GraphExtractor"
	val keyspace = "datalake"
	val tablename = "subject"
	val outputTablename = "graphs"

	// $COVERAGE-OFF$
	/**
	  * Loads subjects from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = sc.cassandraTable[Subject](keyspace, tablename)
		List(subjects).toAnyRDD()
	}

	/**
	  * Writes the generated subgraphs to Cassandra.
	  * @param output first element is the RDD of JSON diffs
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[ResultGraph]()
			.foreach(_.saveToCassandra(keyspace, outputTablename))
	}
	// $COVERAGE-ON$

	/**
	  * Constructs a GraphFrame from the given subjects
	  * @param subjects Input data containing relations
	  * @return GraphFrame containing all the subjects as nodes and the master relations as edges
	  */
	def extractGraph(subjects: RDD[Subject]): GraphFrame = {
		val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._

		val subjectVertices = subjects
			.map(subject => (subject.id.toString, subject.name))
			.toDF("id", "name")
		val subjectRelations = subjects.flatMap(subject => {
			subject.masterRelations.flatMap { case (id, relTypes) =>
				relTypes.map { case (relType, value) =>
					(subject.id.toString, id.toString, relType, value)
				}
			}
		}).toDF("src", "dst", "relationship", "value")

		GraphFrame(subjectVertices, subjectRelations)
	}

	/**
	  * Works on the translated graph using the GraphFrames API
	  * @param graph GraphFrame that is processed
	  * @return RDD of ResultGraph objects that represent extracted subgraphs
	  */
	def processGraph(graph: GraphFrame): List[RDD[ResultGraph]]

	/**
	  * Extract a graph from the master nodes in the subject table and process it.
	  * Checkpoint directory (local on executors) is needed for GraphFrames algorithms like connectedComponents to work
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		sc.setCheckpointDir(appName + "Checkpoints")

		val subjects = input
			.fromAnyRDD[Subject]().head
			.filter(_.datasource == "master")

		val graph = extractGraph(subjects)
		val output = processGraph(graph)
		output.toAnyRDD()
	}
}
