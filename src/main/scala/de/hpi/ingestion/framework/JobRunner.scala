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
import de.hpi.ingestion.implicits.RegexImplicits._
import org.apache.spark.SparkContext

object JobRunner {
	var appName = "Spark Job Runner"
	var jobs = List.empty[SparkJob]

	def main(args: Array[String]): Unit = {
		val job = args.head
		try {
			job match {
				case r"""de\.hpi\.ingestion\.framework\.pipeline\..*""" =>
					val pipeline = Class.forName(job).newInstance.asInstanceOf[JobPipeline]
					pipeline.run()
				case r"""de\.hpi\.ingestion\..*""" =>
					val job = Class.forName(job).newInstance.asInstanceOf[SparkJob]
					val sc = new SparkContext(job.createSparkConf())
					job.execute(sc, args.slice(1, args.length))
			}
		} catch {
			case ce: ClassNotFoundException =>
				Console.err.println(s"There is no such pipeline or job.\n${ce.getMessage}")
			case me: NoSuchMethodException =>
				Console.err.println(s"The provided class is not a Spark Job or a Pipeline.\n${me.getMessage}")
			case ce: ClassCastException =>
				Console.err.println(s"The provided class is not a Spark Job or a Pipeline.\n${ce.getMessage}")
		}
	}
}
