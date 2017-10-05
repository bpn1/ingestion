package de.hpi.ingestion.framework

import org.apache.spark.{SparkConf, SparkContext}

object JobRunner {
	var appName = "Spark Job Runner"
	var jobs = List.empty[SparkJobClass]

	def main(args: Array[String]): Unit = {
		val sc = new SparkContext(new SparkConf().setAppName(appName))
		jobs.foreach(_.execute(sc, args))
	}
}
