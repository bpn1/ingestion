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
