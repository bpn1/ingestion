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

import de.hpi.ingestion.framework.pipeline.JobPipeline
import org.apache.spark.SparkContext

/**
  * Executes a Spark Job or Job Pipeline provided by the arguments. Throws an IllegalArgumentException if the passed
  * arguments do not specify a Spark Job or a Job Pipeline.
  */
object JobRunner {
	/**
	  * Executes a Spark Job or Job Pipeline specified by the first argument and passes the remaining arguments to the
	  * Job. If neither a Spark Job or a Job Pipeline are specified an Exception is thrown.
	  * @param args command line arguments passed to the program
	  * @throws java.lang.IllegalArgumentException thrown if the first argument doesn't specify a Spark Job or a Job
	  * Pipeline
	  */
	@throws(classOf[IllegalArgumentException])
	def main(args: Array[String]): Unit = {
		try {
			val job = Class.forName(args.head).newInstance()
			job match {
				case pipeline: JobPipeline =>
					val sc = SparkContext.getOrCreate(pipeline.createSparkConf())
					pipeline.run(sc)
				case sparkJob: SparkJob =>
					val sc = SparkContext.getOrCreate(sparkJob.createSparkConf())
					sparkJob.execute(sc, args.slice(1, args.length))
			}
		} catch {
			case ce: ClassNotFoundException =>
				throw new IllegalArgumentException("There is no such pipeline or job.", ce)
			case ie: InstantiationException =>
				throw new IllegalArgumentException("The provided class is not a Spark Job or a Pipeline.", ie)
			case me: MatchError =>
				val message = "The provided class does not implement the trait SparkJob or JobPipeline."
				throw new IllegalArgumentException(message, me)
		}
	}
}
