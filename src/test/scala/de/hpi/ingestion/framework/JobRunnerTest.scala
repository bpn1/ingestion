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

import java.io.ByteArrayOutputStream

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class JobRunnerTest extends FlatSpec with Matchers with SharedSparkContext {
    "Jobs" should "be executed" in {
        val output = new ByteArrayOutputStream()
        Console.withOut(output) {
            JobRunner.main(Array("de.hpi.ingestion.framework.mock.MockPrintSparkJob"))
        }
        val expectedOutput = "assertConditions\nexecQ\nload\nrun\nexecQ\nsave\n"
        output.toString shouldEqual expectedOutput
    }

    they should "be passed command line arguments" in {
        val output = new ByteArrayOutputStream()
        Console.withOut(output) {
            JobRunner.main(Array("de.hpi.ingestion.framework.mock.MockPrintSparkJob", "-c", "test.xml"))
        }
        val expectedOutput = "assertConditions\nexecQ\nload\nrun\ntest.xml\nexecQ\nsave\n"
        output.toString shouldEqual expectedOutput
    }

    "Pipeline" should "be executed" in {
        val output = new ByteArrayOutputStream()
        Console.withOut(output) {
            JobRunner.main(Array("de.hpi.ingestion.framework.mock.MockPipeline", "arg"))
        }
        val expectedOutput = "assertConditions\nexecQ\nload\nrun\nexecQ\nsave\nassertConditions\nexecQ\n" +
            "load\nrun\ntest.xml\nexecQ\nsave\n"
        output.toString shouldEqual expectedOutput
    }

    "Invalid input" should "be caught" in {
        val nonExistingClass = "de.hpi.ingestion.abcde"
        val thrown1 = the [IllegalArgumentException] thrownBy JobRunner.main(Array(nonExistingClass))
        thrown1.getCause shouldBe a [ClassNotFoundException]
        thrown1.getMessage shouldEqual "There is no such pipeline or job."

        val nonEmptyConstructor = "de.hpi.ingestion.dataimport.dbpedia.models.Relation"
        val thrown2 = the [IllegalArgumentException] thrownBy JobRunner.main(Array(nonEmptyConstructor))
        thrown2.getCause shouldBe a [InstantiationException]
        thrown2.getMessage shouldEqual "The provided class is not a Spark Job or a Pipeline."

        val notImplementingTrait = "de.hpi.ingestion.textmining.tokenizer.AccessibleGermanStemmer"
        val thrown3 = the [IllegalArgumentException] thrownBy JobRunner.main(Array(notImplementingTrait))
        thrown3.getCause shouldBe a [MatchError]
        thrown3.getMessage shouldEqual "The provided class does not implement the trait SparkJob or JobPipeline."
    }
}
