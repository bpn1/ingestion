package de.hpi.ingestion.framework

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Spark Job framework trait implementing the main method and defining the load, run and save methods.
  */
trait SparkJob extends Configurable {
	var appName = "Ingestion Spark Job"
	val sparkOptions = mutable.Map[String, String]()
	val cassandraLoadQueries = ListBuffer[String]()
	val cassandraSaveQueries = ListBuffer[String]()

	/**
	  * Loads a four-tuple of RDD-Options used in the job.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	def load(sc: SparkContext, args: Array[String]): List[RDD[Any]]

	/**
	  * Executes the data processing of this job and produces the output data.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]]

	/**
	  * Saves the output data to e.g. the Cassandra or the HDFS.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit

	/**
	  * Called before running the job. Used to assert specifics of the input arguments. Returns false if the program
	  * should be terminated. Parses the xml config if a path is given in args or {@configFile} is set.
	  * @param args arguments of the program
	  * @return true if the program can continue, false if it should be terminated
	  */
	def assertConditions(args: Array[String]): Boolean = {
		args
			.headOption
			.foreach(file => configFile = file)
		if(configFile.nonEmpty) {
			parseConfig()
		}
		true
	}

	/**
	  * Creates the Spark Conf and sets extra options. {@appName} is used as name and {@sparkOptions} is used as
	  * extra Spark options.
	  * @return Spark Conf with the extra values set.
	  */
	def createSparkConf(): SparkConf = {
		new SparkConf()
			.setAppName(appName)
			.setAll(sparkOptions.toList)
	}

	// $COVERAGE-OFF$
	/**
	  * Creates a Spark Context used for the rest of the program. Can be overwritten for e.g. tests.
	  * @return Spark Context used for the rest of the job
	  */
	def sparkContext(): SparkContext = {
		new SparkContext(createSparkConf())
	}

	/**
	  * Executes multiple CQL queries on the Cassandra.
	  * @param queries List of queries to execute
	  * @param sc Spark Context containing the Cassandra connection
	  */
	def executeQueries(queries: List[String], sc: SparkContext): Unit = {
		CassandraConnector(sc.getConf).withSessionDo(session => queries.foreach(session.execute))
	}
	// $COVERAGE-ON$

	/**
	  * Spark job main method which first asserts definable conditions and then loads, processes and saves the data.
	  * @param args arguments of the program
	  */
	def main(args: Array[String]): Unit = {
		if(!assertConditions(args)) {
			return
		}
		val sc = sparkContext()
		executeQueries(cassandraLoadQueries.toList, sc)
		val data = load(sc, args)
		val results = run(data, sc, args)
		executeQueries(cassandraSaveQueries.toList, sc)
		save(results, sc, args)
	}
}
