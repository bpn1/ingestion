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
import de.hpi.ingestion.framework.mock.{MockConditionSparkJob, MockSparkJob}
import de.hpi.ingestion.textmining.{TestData => TextTestData}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

class SparkJobTest extends FlatSpec with Matchers with SharedSparkContext {
    "Execute" should "call the methods in the proper sequence" in {
        val sparkJob = new MockSparkJob
        sparkJob.execute(sc)
        val expectedSequence = List("assertConditions", "execQ", "load", "run", "execQ", "save")
        sparkJob.methodCalls.toList shouldEqual expectedSequence
    }

    "Assert conditions" should "not stop the job if the conditions are true" in {
        val sparkJob = new MockConditionSparkJob
        sparkJob.execute(sc, Array("-c", "test.xml"))
        val expectedSequence = List("assertConditions", "execQ", "load", "run" , "execQ", "save")
        sparkJob.methodCalls.toList shouldEqual expectedSequence
    }

    it should "stop the job if the conditions are false" in {
        val sparkJob = new MockConditionSparkJob
        sparkJob.execute(sc)
        val expectedSequence = List("assertConditions")
        sparkJob.methodCalls.toList shouldEqual expectedSequence
    }

    "Spark Conf options" should "be set to the correct values" in {
        val sparkJob = new MockSparkJob
        val testName = "Mock Spark Job"
        val testOptions = Map("option1" -> "value1", "option2" -> "value2")
        sparkJob.appName = testName
        sparkJob.sparkOptions ++= testOptions
        val conf = sparkJob.createSparkConf()
        conf.get("spark.app.name") shouldEqual testName
        conf.get("option1") shouldEqual testOptions("option1")
        conf.get("option2") shouldEqual testOptions("option2")
    }

    "Config" should "be read before run is executed" in {
        val sparkJob = new MockSparkJob
        sparkJob.settings shouldBe empty
        sparkJob.assertConditions()
        sparkJob.settings shouldBe empty
        sparkJob.configFile = "test.xml"
        sparkJob.assertConditions()
        sparkJob.settings should not be empty
        sparkJob.scoreConfigSettings should not be empty
        sparkJob.importConfigFile = "normalization_wikidata.xml"
        sparkJob.assertConditions()
        sparkJob.normalizationSettings should not be empty
        sparkJob.sectorSettings should not be empty
    }

    it should "be read from the command line config" in {
        val sparkJob = new MockSparkJob
        sparkJob.settings shouldBe empty
        sparkJob.assertConditions()
        sparkJob.settings shouldBe empty
        sparkJob.conf = CommandLineConf(Seq("-c", "test.xml", "-i", "normalization_wikidata.xml"))
        sparkJob.assertConditions()
        sparkJob.settings should not be empty
        sparkJob.scoreConfigSettings should not be empty
        sparkJob.normalizationSettings should not be empty
        sparkJob.sectorSettings should not be empty
    }

    it should "be the file passed as argument" in {
        val sparkJob = new MockSparkJob
        sparkJob.settings shouldBe empty
        sparkJob.assertConditions()
        sparkJob.settings shouldBe empty
        sparkJob.conf = CommandLineConf(Seq("--config", "test.xml"))
        sparkJob.assertConditions()
        sparkJob.settings should not be empty
    }

    "Cassandra queries" should "be called" in {
        val sparkJob = new MockSparkJob
        val loadQueries = List("loadQuery 1", "loadQuery 2")
        val saveQueries = List("saveQuery1")
        sparkJob.cassandraLoadQueries ++= loadQueries
        sparkJob.cassandraSaveQueries ++= saveQueries
        sparkJob.execute(sc)
        val expectedSequence = List("loadQuery 1", "loadQuery 2", "saveQuery1")
        sparkJob.queryCalls.toList shouldEqual expectedSequence
    }

    "Files" should "be read and parsed from any Input Stream" in {
        val sparkJob = new MockSparkJob
        val fileStreamFunction = TextTestData.docfreqStream("docfreq2") _
        val parseLineFunction: String => Option[(String, Int)] = { line =>
            val Array(count, word) = line.split("\t")
            Option((word, count.toInt)).filter(_._2 < 3)
        }
        val transformCollectionFunction: List[(String, Int)] => Map[String, Int] = { lineList => lineList.toMap }
        val parsedMap = sparkJob.parseHDFSFileToCollection(
            "name",
            fileStreamFunction,
            parseLineFunction,
            transformCollectionFunction)
        parsedMap shouldEqual Map("verlink" -> 2, "hier" -> 2, "ist" -> 2)

        val parseLineFunctionDouble: String => Option[Double] = { line =>
            val Array(count, word) = line.split("\t")
            Option(count.toDouble / word.length)
        }
        val transformCollectionFunctionDouble: List[Double] => Double = { lineList => lineList.sum / lineList.length }
        val parsedAverage = sparkJob.parseHDFSFileToCollection(
            "name",
            fileStreamFunction,
            parseLineFunctionDouble,
            transformCollectionFunctionDouble)
        parsedAverage === (185.0 / 336.0) +- 0.000000000000001 shouldBe true
    }

    they should "be parsed into a mutable collection" in {
        val sparkJob = new MockSparkJob
        val fileStreamFunction = TextTestData.docfreqStream("docfreq2") _
        val createCollectionFunction = () => mutable.Map.empty[String, Int]
        val parseLineFunction: (String, mutable.Map[String, Int]) => Unit = { case (line, collection) =>
            val Array(count, word) = line.split("\t")
            if(count.toInt < 3) {
                collection(word) = count.toInt
            }
        }
        val parsedMap = sparkJob.parseHDFSFileToMutableCollection(
            "name",
            fileStreamFunction,
            createCollectionFunction,
            parseLineFunction)
        parsedMap shouldEqual mutable.Map("verlink" -> 2, "hier" -> 2, "ist" -> 2)
    }
}
