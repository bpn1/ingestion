package de.hpi.ingestion.framework

import org.apache.spark.rdd.RDD

trait SplitSparkJob extends SparkJob {

	/**
	  * Splits the input into a number of parts which are executed and saved sequentially.
	  * @param input List of RDDs containing the input data
	  * @param args arguments of the program
	  * @return Collection of input RDD Lists to be processed sequentially.
	  */
	def splitInput(input: List[RDD[Any]], args: Array[String]): Traversable[List[RDD[Any]]]

	/**
	  * Spark job main method which first asserts definable conditions and then loads, processes and saves the data.
	  * @param args arguments of the program
	  */
	override def main(args: Array[String]): Unit = {
		if(!assertConditions(args)) {
			return
		}
		val sc = sparkContext()
		executeQueries(cassandraLoadQueries.toList, sc)
		val data = load(sc, args)
		executeQueries(cassandraSaveQueries.toList, sc)
		splitInput(data, args).foreach { input =>
			val results = run(input, sc, args)
			save(results, sc, args)
		}
	}
}
