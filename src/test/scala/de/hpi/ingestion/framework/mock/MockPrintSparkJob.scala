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

import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext

class MockPrintSparkJob extends SparkJob {
	configFile = "test.xml"
	override def assertConditions(): Boolean = {
		println("assertConditions")
		super.assertConditions()
	}

	override def load(sc: SparkContext): Unit = println("load")
	override def run(sc: SparkContext): Unit = {
		println("run")
		args.foreach(println)
	}
	override def save(sc: SparkContext): Unit = println("save")
	override def executeQueries(sc: SparkContext, queries: List[String]): Unit = println("execQ")
}