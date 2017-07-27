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

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.framework.mock.MockSplitSparkJob
import org.scalatest.{FlatSpec, Matchers}

class SplitSparkJobTest extends FlatSpec with Matchers with SharedSparkContext {

	"Main method" should "call the methods in the proper sequence" in {
		val sparkJob = new MockSplitSparkJob
		sparkJob.main(Array())
		val expectedSequence = List("assertConditions", "sparkContext", "execQ", "load", "execQ", "split",
			"run", "save", "run", "save", "run", "save")
		sparkJob.methodCalls.toList shouldEqual expectedSequence
	}

	it should "return if the conditions are false" in {
		val sparkJob = new MockSplitSparkJob
		sparkJob.main(Array("test"))
		val expectedSequence = List("assertConditions")
		sparkJob.methodCalls.toList shouldEqual expectedSequence
	}
}
