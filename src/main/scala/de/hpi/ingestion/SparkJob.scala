package de.hpi.ingestion

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark Job framework trait implementing the main method and defining the load, run and save methods.
  */
trait SparkJob {
	var appName = "Ingestion Spark Job"
	var sparkOptions = Map[String, String]()

	/**
	  * Loads a list of RDDs used in the job
	  * @param sc Spark Context used to load the RDDs
	  * @return List of RDDs containing the data processed in the job.
	  */
	def load(sc: SparkContext): List[RDD[Product]]

	/**
	  * Executes the data processing of this job and produces the output data.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @return List of RDDs containing the output data
	  */
	def run(input: List[RDD[Product]], sc: SparkContext): List[RDD[Product]]

	/**
	  * Saves the output data to e.g. the Cassandra or the HDFS.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	def save(output: List[RDD[Product]], sc: SparkContext): Unit

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName(appName)
			.setAll(sparkOptions.toList)
		val sc = new SparkContext(conf)
		val data = load(sc)
		val results = run(data, sc)
		save(results, sc)
		sc.stop
	}
}
