package de.hpi.ingestion.framework

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark Job framework trait implementing the main method and defining the load, run and save methods.
  */
trait SparkJob {
	var appName = "Ingestion Spark Job"
	var sparkOptions = Map[String, String]()

	/**
	  * Loads a four-tuple of RDD-Options used in the job.
	  * @param sc Spark Context used to load the RDDs
	  * @return List of RDDs containing the data processed in the job.
	  */
	def load(sc: SparkContext): List[RDD[Any]]

	/**
	  * Executes the data processing of this job and produces the output data.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]]

	/**
	  * Saves the output data to e.g. the Cassandra or the HDFS.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	def save(output: List[RDD[Any]], sc: SparkContext): Unit

	/**
	  * Called before running the job. Used to assert specifics of the input arguments. Returns false if the program
	  * should be terminated.
	  * @param args arguments of the program
	  * @return true of the program can continue, false if it should be terminated
	  */
	def assertConditions(args: Array[String]): Boolean = true

	/**
	  * Creates a Spark Context used for the rest of the program. Can be overwritten for e.g. tests.
	  * It will use {@appName} as name and {@sparkOptions} as extra Spark options.
	  * @return Spark Context used for the rest of the job
	  */
	def sparkContext(): SparkContext = {
		val conf = new SparkConf()
			.setAppName(appName)
			.setAll(sparkOptions.toList)
		new SparkContext(conf)
	}

	/**
	  * Spark job main method which first asserts definable conditions and then loads, processes and saves the data.
	  * @param args arguments of the program
	  */
	def main(args: Array[String]): Unit = {
		if(!assertConditions(args)) {
			return
		}
		val sc = sparkContext()
		val data = load(sc)
		val results = run(data, sc, args)
		save(results, sc)
	}
}
