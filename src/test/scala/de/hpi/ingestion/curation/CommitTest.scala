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

package de.hpi.ingestion.curation

import java.util.UUID

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.framework.CommandLineConf
import org.scalatest.{FlatSpec, Matchers}

class CommitTest extends FlatSpec with Matchers with SharedSparkContext {
    "Commit JSON" should "be parsed and processed" in {
        val job = new Commit
        job.subjects = sc.parallelize(TestData.subjects)
        job.conf = CommandLineConf(Seq("-j", TestData.commitJson))
        job.run(sc)
        val updatedSubjects = job.curatedSubjects.collect.toSet
        updatedSubjects should have size 3
        updatedSubjects.filter(_.isMaster) should have size 1
    }

    "New Subject and its master" should "be created" in {
        val job = new Commit
        assert(false)
    }

    "Subject update" should "create a human Subject with the updated data" in {
        val job = new Commit
        assert(false)
    }

    "Subject deletion" should "create human Subject with the deleted flag" in {
        val job = new Commit
        val masterId = UUID.fromString("217b2436-255e-447f-8740-f7d353560cc3")
        val deletedSubject = job.deleteSubject(masterId, TestData.version(sc))
        val expectedSubject = TestData.deletedSubject(masterId)
        deletedSubject.datasource shouldEqual expectedSubject.datasource
        deletedSubject.properties shouldEqual expectedSubject.properties
        deletedSubject.relations shouldEqual expectedSubject.relations
    }

    "Relations" should "be extracted from JSON data" in {
        val job = new Commit
        val extractedRelations = job.extractJSONRelations(TestData.relationJSON)
        val expectedRelations = TestData.extractedRelations
        extractedRelations shouldEqual expectedRelations
    }
}
