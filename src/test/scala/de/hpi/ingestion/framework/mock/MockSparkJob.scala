package de.hpi.ingestion.framework.mock

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec

import scala.collection.mutable.ListBuffer

class MockSparkJob extends FlatSpec with SparkJob with SharedSparkContext {

	val methodCalls = ListBuffer[String]()

	override def sparkContext(): SparkContext = {
		methodCalls += "sparkContext"
		sc
	}

	override def assertConditions(args: Array[String]): Boolean = {
		methodCalls += "assertConditions"
		super.assertConditions(args)
	}

	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		methodCalls += "load"
		Nil
	}

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		methodCalls += "run"
		Nil
	}

	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		methodCalls += "save"
	}
}
