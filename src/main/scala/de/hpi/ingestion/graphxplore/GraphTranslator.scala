package de.hpi.ingestion.graphxplore

import java.util.UUID

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models.Subject
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.graphxplore.models.ResultGraph
import de.hpi.ingestion.implicits.CollectionImplicits._

object GraphTranslator extends SparkJob {
	appName = "GraphTranslator"
	val keyspace = "datalake"
	val tablename = "subject"
	val outputTablename = "graphs"
	val graphType = "ConnectedComponent"

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
	  * Writes the generated subgraphs to Cassandra.
	  * @param output first element is the RDD of JSON diffs
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[ResultGraph]()
			.head
			.saveToCassandra(keyspace, outputTablename)
	}
	// $COVERAGE-ON$

	def processGraph(graph: GraphFrame): RDD[ResultGraph] = {
		graph.connectedComponents.run()
			.select("id", "component")
			.rdd
			.map(row => (row.getAs[Long]("component"), UUID.fromString(row.getAs[String]("id"))))
			.groupBy(_._1)
			.map(_._2.map(_._2).toList)
			.map(idList => ResultGraph(graphType, idList))
	}

	/**
	  * Creates a diff for each subject containing the deletions and additions of every field between the two versions.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val spark = SparkSession
			.builder()
			.appName(appName)
			.config(sc.getConf)
			.getOrCreate()
		import spark.implicits._

		// TODO find out correct HDFS path (something like hdfs://localhost:PORT/graph_translator)
		sc.setCheckpointDir("graph_translator")

		val subjects = input
			.fromAnyRDD[Subject]()
			.head
			.filter(_.datasource == "master")
		val subjectVertices = subjects
			.map(subject => (subject.id.toString, subject.name))
			.toDF("id", "name")
		val subjectRelations = subjects.flatMap(subject => {
			subject.relations.flatMap { case (id, relTypes) =>
				relTypes.map { case (relType, value) =>
					(subject.id.toString, id.toString, relType, value)
				}
			}
		}).toDF("src", "dst", "relationship", "value")

		val graph = GraphFrame(subjectVertices, subjectRelations)
		val output = processGraph(graph)
		List(output).toAnyRDD()
	}
}
