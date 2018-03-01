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

package de.hpi.ingestion.datalake

import java.io.ByteArrayOutputStream

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class ExtractedRelationMatchingTest extends FlatSpec with Matchers with SharedSparkContext {
    "Extracted relations" should "be matched to Subjects" in {
        val job = new ExtractedRelationMatching
        job.relations = sc.parallelize(TestData.extractedRelations())
        job.subjects = sc.parallelize(TestData.relationMatchingSubjects())
        job.run(sc)
        val matchedRelations = job.matchedRelations.collect.toSet
        val expectedRelations = TestData.importRelations().toSet
        matchedRelations shouldEqual expectedRelations
    }

    they should "be matched for duplicate Subjects with the same name" in {
        val job = new ExtractedRelationMatching
        job.relations = sc.parallelize(TestData.extractedRelations())
        job.subjects = sc.parallelize(TestData.relationMatchingDuplicates())
        job.run(sc)
        val matchedRelations = job.matchedRelations.collect.toSet
        matchedRelations should have size 2
        val destinationIds = Set(
            TestData.relationMatchingDuplicateIdMap("Porsche_implisense"),
            TestData.relationMatchingDuplicateIdMap("Porsche_dbpedia")
        )
        matchedRelations.foreach { relation =>
            relation.start shouldEqual TestData.relationMatchingDuplicateIdMap("VW")
            destinationIds should contain (relation.destination)
        }
    }

    they should "be removed if there is no match" in {
        val job = new ExtractedRelationMatching
        job.relations = sc.parallelize(TestData.extractedRelations())
        job.subjects = sc.parallelize(TestData.relationMatchingSubjectsNoMatch())
        job.run(sc)
        val matchedRelations = job.matchedRelations.collect.toSet
        matchedRelations should have size 1
        matchedRelations should contain (TestData.singleImportRelation())

        job.subjects = sc.parallelize(Nil)
        job.run(sc)
        job.matchedRelations.collect.toSet shouldBe empty
    }

    they should "be removed and printed if there is no unique match" in {
        val output = new ByteArrayOutputStream()
        Console.withOut(output) {
            val job = new ExtractedRelationMatching
            job.relations = sc.parallelize(TestData.extractedRelations())
            job.subjects = sc.parallelize(TestData.relationMatchingSubjectsNoUniqueMatch())
            job.run(sc)
            val matchedRelations = job.matchedRelations.collect.toSet
            matchedRelations should have size 1
            matchedRelations should contain (TestData.singleImportRelation())
        }
        val outputString = output.toString()
        outputString should not be empty
        outputString.split("\n").toList should have length 6
    }
}
