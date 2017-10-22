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

package de.hpi.ingestion.framework.pipeline

import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.{SparkConf, SparkContext}

trait JobPipeline {
	var jobs: List[(SparkJob, Array[String])] = Nil
	var pipelineName: String = "Pipeline"

	def createSparkConf(): SparkConf = {
		val sparkOptions = jobs
			.flatMap(_._1.sparkOptions.toList)
			.groupBy(_._1)
			.map { case (property, values) =>
				val propValues = values.map(_._2)
				val propValue = property match {
					case "spark.yarn.executor.memoryOverhead" => propValues.map(_.toInt).max.toString
					case "spark.kryo.registrator" | _ => propValues.head
				}
				(property, propValue)
			}
		new SparkConf()
			.setAppName(pipelineName)
			.setAll(sparkOptions.toList)
	}

	def run(sc: SparkContext): Unit = {
		jobs.foreach { case (job, args) => job.execute(sc, args) }
	}
}
