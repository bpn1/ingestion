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
import de.hpi.ingestion.framework.mock.{MockPipeline, MockSparkJob}
import de.hpi.ingestion.framework.pipeline._
import org.scalatest.{FlatSpec, Matchers}

class PipelineTest extends FlatSpec with Matchers with SharedSparkContext {
    "Spark Conf" should "be merged and created" in {
        val job1 = new MockSparkJob
        job1.sparkOptions("spark.yarn.executor.memoryOverhead") = "4096"
        job1.sparkOptions("spark.kryo.registrator") = "de.hpi.ingestion.textmining.kryo.TrieKryoRegistrator"
        val job2 = new MockSparkJob
        job2.sparkOptions("spark.yarn.executor.memoryOverhead") = "2048"
        job2.sparkOptions("spark.kryo.registrationRequired") = "true"
        val pipeline = new MockPipeline
        pipeline.jobs = List(job1, job2)
        val sparkConf = pipeline.createSparkConf().getAll.toMap
        val expectedSparkConf = Map(
            "spark.app.name" -> "Mock Pipeline",
            "spark.yarn.executor.memoryOverhead" -> "4096",
            "spark.kryo.registrator" -> "de.hpi.ingestion.textmining.kryo.TrieKryoRegistrator",
            "spark.kryo.registrationRequired" -> "true")
        sparkConf shouldEqual expectedSparkConf
    }

    "Pipeline jobs" should "be executed" in {
        val pipeline = new MockPipeline
        pipeline.jobs = List(new MockSparkJob, new MockSparkJob)
        pipeline.run(sc)
        val List(job1, job2) = pipeline.jobs.map(_.asInstanceOf[MockSparkJob])
        job1.methodCalls.toList shouldEqual List("assertConditions", "execQ", "load", "run", "execQ", "save")
        job2.methodCalls.toList shouldEqual List("assertConditions", "execQ", "load", "run", "execQ", "save")
    }

    "Textmining Pipeline" should "have the correct name and number of jobs" in {
        val pipeline = new TextminingPipeline
        pipeline.createSparkConf().get("spark.app.name") shouldEqual "Textmining Pipeline"
        pipeline.jobs should have length 16
    }

    "Implisense Pipeline" should "have the correct name and number of jobs" in {
        val pipeline = new ImplisensePipeline
        pipeline.createSparkConf().get("spark.app.name") shouldEqual "Implisense Pipeline"
        pipeline.jobs should have length 2
    }

    "Wikidata Pipeline" should "have the correct name and number of jobs" in {
        val pipeline = new WikidataPipeline
        pipeline.createSparkConf().get("spark.app.name") shouldEqual "Wikidata Pipeline"
        pipeline.jobs should have length 5
    }

    "DBpedia Pipeline" should "have the correct name and number of jobs" in {
        val pipeline = new DBpediaPipeline
        pipeline.createSparkConf().get("spark.app.name") shouldEqual "DBpedia Pipeline"
        pipeline.jobs should have length 4
    }

    "Kompass Pipeline" should "have the correct name and number of jobs" in {
        val pipeline = new KompassPipeline
        pipeline.createSparkConf().get("spark.app.name") shouldEqual "Kompass Pipeline"
        pipeline.jobs should have length 2
    }

    "Commit Pipeline" should "have the correct name and number of jobs" in {
        val pipeline = new CommitPipeline
        pipeline.createSparkConf().get("spark.app.name") shouldEqual "Curation Commit Pipeline"
        pipeline.jobs should have length 3
    }
}
