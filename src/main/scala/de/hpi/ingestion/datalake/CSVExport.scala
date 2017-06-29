package de.hpi.ingestion.datalake

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._

object CSVExport extends SparkJob {
	appName = "CSVExport_v1.0"
	val keyspace = "datalake"
	val tablename = "subject"
	val quote = "\""
	val separator = ","
	val arraySeparator = ";"

	// $COVERAGE-OFF$
	/**
	  * Loads Subjects from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = sc.cassandraTable[Subject](keyspace, tablename)
		List(subjects).toAnyRDD()
	}

	/**
	  * Saves nodes and edges to the HDFS.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		val List(nodes, edges) = output.fromAnyRDD[String]()
		val timestamp = System.currentTimeMillis / 1000
		nodes.saveAsTextFile(s"export_nodes_$timestamp")
		edges.saveAsTextFile(s"export_edges_$timestamp")
	}
	// $COVERAGE-ON$

	/**
	  * Parses a Subject into a csv node in the following format:
	  * :ID(Subject),name,aliases:string[],category:LABEL
	  * @param subject Subject to parse
	  * @return a comma separated line containing the id, name, aliases and category of the Subject
	  */
	def nodeToCSV(subject: Subject): String = {
		val aliasString = subject.aliases
			.mkString(arraySeparator)
			.replace("\\", "")
			.replace("\"", "\\\"")
		val name = subject.name.getOrElse("").replace("\"", "'")
		val output = List(subject.id.toString, name, aliasString, subject.category.getOrElse(""))
			.mkString(quote + separator + quote)
		// TODO serialize properties to JSON string
		quote + output + quote
	}

	/**
	  * Parses a Subjects relations to multiple csv edges in the following format:
	  * :START_ID(Subject),:END_ID(Subject),:TYPE
	  * @param subject Subject to parse
	  * @return List of comma separated lines of subject id, target id, relation type
	  */
	def edgesToCSV(subject: Subject): List[String] = {
		subject.relations.map { case (id, props) =>
			val relType = props.getOrElse("type", "")
			subject.id + separator + id + separator + relType
		}.map(_.trim)
		.filter(_.nonEmpty)
		.toList
	}

	/**
	  * Creates Nodes and Edges from the Subjects.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val subjects = input.fromAnyRDD[Subject]().head
		val nodes = subjects.map(nodeToCSV)
		val edges = subjects.flatMap(edgesToCSV)
		List(nodes, edges).toAnyRDD()
	}
}
