package de.hpi.ingestion.framework.mock

import de.hpi.ingestion.framework.SparkJobClass
import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer

class MockSparkJobClass extends SparkJobClass {
	val methodCalls = ListBuffer[String]()
	val queryCalls = ListBuffer[String]()

	override def assertConditions(): Boolean = {
		methodCalls += "assertConditions"
		super.assertConditions()
	}

	override def load(sc: SparkContext): Unit = methodCalls += "load"
	override def run(sc: SparkContext): Unit = methodCalls += "run"
	override def save(sc: SparkContext): Unit = methodCalls += "save"

	override def executeQueries(sc: SparkContext, queries: List[String]): Unit = {
		queryCalls ++= queries
		methodCalls += "execQ"
	}
}
