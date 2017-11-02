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

package de.hpi.ingestion.deduplication.similarity

import org.scalatest.{FlatSpec, Matchers}

class ExactMatchUnitTest extends FlatSpec with Matchers {

    "compare" should "return 1.0 or 0.0 for given strings" in {
        val testData = List(
            ("tags", "nachts", 0.0),
            ("context", "context", 1.0))

        testData.foreach(tuple =>
            ExactMatchString.compare(tuple._1, tuple._2) shouldEqual tuple._3)
    }

    it should "return 1.0 or 0.0 for given doubles" in {
        val testData = List(
            (0.3, 0.3, 1.0),
            (0.2, 0.4, 0.0))

        testData.foreach(tuple =>
            ExactMatchDouble.compare(tuple._1, tuple._2) shouldEqual tuple._3)
    }
}
