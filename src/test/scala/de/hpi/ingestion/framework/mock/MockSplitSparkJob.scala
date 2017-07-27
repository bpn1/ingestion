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

package de.hpi.ingestion.framework.mock

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.framework.SplitSparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec
import scala.collection.mutable.ListBuffer

class MockSplitSparkJob extends FlatSpec with SplitSparkJob with SharedSparkContext {

	val methodCalls = ListBuffer[String]()
	val queryCalls = ListBuffer[String]()

	override def sparkContext(): SparkContext = {
		methodCalls += "sparkContext"
		sc
	}

	override def assertConditions(args: Array[String]): Boolean = {
		methodCalls += "assertConditions"
		args.isEmpty
	}

	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		methodCalls += "load"
		Nil
	}

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		methodCalls += "run"
		Nil
	}

	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		methodCalls += "save"
	}

	override def executeQueries(queries: List[String], sc: SparkContext): Unit = {
		queryCalls ++= queries
		methodCalls += "execQ"
	}

	override def splitInput(input: List[RDD[Any]], args: Array[String]): Traversable[List[RDD[Any]]] = {
		methodCalls += "split"
		(0 until 3).map(t => Nil)
	}
}
