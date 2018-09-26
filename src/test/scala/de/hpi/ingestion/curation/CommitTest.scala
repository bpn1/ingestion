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
import play.api.libs.json.JsValue

class CommitTest extends FlatSpec with Matchers with SharedSparkContext {
    "Commit JSON" should "be parsed and processed" in {
        val job = new Commit
        job.subjects = sc.parallelize(TestData.subjects)
        job.conf = CommandLineConf(Seq("-j", TestData.commitJSON))
        job.run(sc)
        val updatedSubjects = job.curatedSubjects.collect.toSet
        updatedSubjects should have size 4
        updatedSubjects.filter(_.isMaster) should have size 1
    }

    "New Subject and its master" should "be created" in {
        val job = new Commit
        val createdSubjects = job.createSubject(TestData.createJSON, TestData.version(sc))
        val (List(createdMaster), List(createdSlave)) = createdSubjects.partition(_.isMaster)
        val expectedSubject = TestData.createdSubject

        createdSlave.master shouldEqual expectedSubject.master
        createdSlave.datasource shouldEqual expectedSubject.datasource
        createdSlave.name shouldEqual expectedSubject.name
        createdSlave.category shouldEqual expectedSubject.category
        createdSlave.aliases shouldEqual expectedSubject.aliases
        createdSlave.properties shouldEqual expectedSubject.properties
        createdSlave.relations shouldEqual expectedSubject.relations

        createdMaster.id shouldEqual createdSlave.master
    }

    "Subject update" should "create human Subjects with the updated data" in {
        val job = new Commit
        val updatedSubjects = TestData.subjectUpdate.map { case (oldSubject, updateJSON) =>
            job.updateSubject(
                masterSubject = oldSubject,
                subjectJson = updateJSON,
                version = TestData.version(sc),
                relationUpdateTargets = TestData.subjectUpdateTargetSlaves
            )
        }
        val expectedSubjects = TestData.updatedSubjects

        updatedSubjects should have length expectedSubjects.length
        updatedSubjects.zip(expectedSubjects).foreach { case (updatedSubject, expectedSubject) =>
            updatedSubject.id should not equal updatedSubject.master
            updatedSubject.master shouldEqual expectedSubject.master
            updatedSubject.datasource shouldEqual expectedSubject.datasource
            updatedSubject.name shouldEqual expectedSubject.name
            updatedSubject.category shouldEqual expectedSubject.category
            updatedSubject.aliases shouldEqual expectedSubject.aliases
            updatedSubject.properties shouldEqual expectedSubject.properties
            updatedSubject.relations shouldEqual expectedSubject.relations
        }
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

    "Relations" should "be extracted from the JSON data" in {
        val job = new Commit
        val extractedRelations = job.extractRelations(TestData.relationJSON)
        val expectedRelations = TestData.extractedRelations
        extractedRelations shouldEqual expectedRelations
    }

    they should "be redirected" in {
        val job = new Commit
        val redirectedRelations = job.redirectRelations(
            TestData.relationsToMasters,
            TestData.relationSlaves,
            TestData.relationTargetSlaves
        )
        val expectedRelations = TestData.redirectedRelations
        redirectedRelations shouldEqual expectedRelations
    }

    "Aliases" should "be extracted from the JSON data" in {
        val job = new Commit
        val aliases = job.extractAliases(TestData.aliasJSON)
        val expectedAliases = TestData.extractedAliases
        aliases shouldEqual expectedAliases
    }

    "Properties" should "be extracted from the JSON data" in {
        val job = new Commit
        val properties = TestData.propertyJSON
            .as[Map[String, JsValue]]
            .map((job.extractProperty _).tupled)
        val expectedProperties = TestData.extractedProperties
        properties shouldEqual expectedProperties
    }

    they should "be extracted correctly" in {
        val job = new Commit
        val extractedProperties = TestData.jsonPropertiesToExtract
            .map(job.extractProperty("key", _))
            .map(_._2)
        val expectedProperties = TestData.extractedJSONProperties
        extractedProperties shouldEqual expectedProperties
    }
}
