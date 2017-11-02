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

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.datalake.models._
import org.scalatest.{FlatSpec, Matchers}

class SubjectManagerTest extends FlatSpec with Matchers with SharedSparkContext {
    "buildDuplicatesSCC" should "should add isDuplicate relation with a confidence for a list of tuples" in {
        val subjects = TestData.subjects
        val duplicates = List((subjects.head, subjects(2), 0.8), (subjects(1), subjects(3), 0.8))
        val expectedRelation = Map("isDuplicate" -> "0.8")
        val expectedRelationNode1 = Map(subjects(2).id -> expectedRelation)
        val expectedRelationNode2 = Map(subjects(3).id -> expectedRelation)
        val version = TestData.version(sc)
        SubjectManager.buildDuplicatesSCC(duplicates, version)

        subjects.head.relations shouldEqual expectedRelationNode1
        subjects(1).relations shouldEqual expectedRelationNode2
    }

    "addSymRelation" should "add a symmetric relation between two given nodes" in {
        val subjects = TestData.subjects
        val relation = Map("isDuplicate" -> "0.8")
        val version = TestData.version(sc)
        val expectedRelations = List(
            Map(subjects(1).id -> relation),
            Map(subjects.head.id -> relation))
        SubjectManager.addSymRelation(subjects.head, subjects(1), relation, version)
        subjects.zip(expectedRelations).foreach { case (subject, expectedRelation) =>
            subject.relations shouldEqual expectedRelation
        }
    }

    "Aliases" should "be added and removed" in {
        val subject = TestData.subject
        val version = Version.apply("SM Test", Nil, sc, false, None)
        val sm = new SubjectManager(subject, version)
        sm.addAliases(List("alias 1", "alias 2"))
        subject.aliases shouldEqual List("alias 1", "alias 2")
        subject.aliases_history should have length 1
        subject.aliases_history.head.value shouldEqual List("alias 1", "alias 2")

        sm.removeAliases(List("alias 1"))
        subject.aliases shouldEqual List("alias 2")
        subject.aliases_history should have length 2
        subject.aliases_history.head.value shouldEqual List("alias 1", "alias 2")
        subject.aliases_history.last.value shouldEqual List("alias 2")

        sm.setAliases(List("alias 2"))
        subject.aliases shouldEqual List("alias 2")
        subject.aliases_history should have length 2
    }

    "Name" should "be set" in {
        val subject = TestData.subject
        val version = Version.apply("SM Test", Nil, sc, false, None)
        val sm = new SubjectManager(subject, version)
        sm.setName("test name")
        subject.name should contain ("test name")
        subject.name_history should have length 1
        subject.name_history.head.value shouldEqual List("test name")

        sm.setName("test name")
        subject.name_history should have length 1
        sm.setName(Option("test name"))
        subject.name_history should have length 1

        sm.setName(Option("test name 2"))
        subject.name should contain ("test name 2")
        subject.name_history should have length 2
        subject.name_history.last.value shouldEqual List("test name 2")
    }

    "Category" should "be set" in {
        val subject = TestData.subject
        val version = Version.apply("SM Test", Nil, sc, false, None)
        val sm = new SubjectManager(subject, version)
        sm.setCategory("test category")
        subject.category should contain ("test category")
        subject.category_history should have length 1
        subject.category_history.head.value shouldEqual List("test category")

        sm.setCategory("test category")
        subject.category_history should have length 1
        sm.setCategory(Option("test category"))
        subject.category_history should have length 1

        sm.setCategory(Option("test category 2"))
        subject.category should contain ("test category 2")
        subject.category_history should have length 2
        subject.category_history.last.value shouldEqual List("test category 2")
    }

    "Master node" should "be set" in {
        val subject = TestData.subject
        val version = Version("Subject Manager Test", Nil, sc, false, None)
        val List(masterId, masterId2) = TestData.masterIds()
        val sm = new SubjectManager(subject, version)

        sm.setMaster(masterId, 0.5)
        subject.master shouldEqual masterId
        subject.masterScore should contain(0.5)
        subject.master_history should have length 1
        subject.master_history.head.value shouldEqual List(masterId.toString)
        subject.relations(masterId) shouldEqual SubjectManager.slaveRelation(0.5)
        subject.relations_history(masterId)(SubjectManager.slaveKey) should have length 1
        subject.relations_history(masterId)(SubjectManager.slaveKey).head.value shouldEqual List("0.5")

        sm.setMaster(masterId, 0.3)
        subject.master_history should have length 1
        subject.masterScore should contain(0.5)
        subject.relations_history(masterId)(SubjectManager.slaveKey) should have length 1
        subject.relations_history(masterId)(SubjectManager.slaveKey).last.value shouldEqual List("0.5")

        sm.setMaster(masterId, 1.0)
        subject.master_history should have length 1
        subject.masterScore should contain(1.0)
        subject.relations(masterId) shouldEqual SubjectManager.slaveRelation(1.0)
        subject.relations_history(masterId)(SubjectManager.slaveKey) should have length 2
        subject.relations_history(masterId)(SubjectManager.slaveKey).last.value shouldEqual List("1.0")

        sm.setMaster(masterId2, 0.5)
        subject.master shouldEqual masterId2
        subject.masterScore should contain(0.5)
        subject.master_history should have length 2
        subject.master_history.last.value shouldEqual List(masterId2.toString)
        subject.relations.get(masterId) shouldBe empty
        subject.relations(masterId2) shouldEqual SubjectManager.slaveRelation(0.5)
        subject.relations_history(masterId)(SubjectManager.slaveKey) should have length 3
        subject.relations_history(masterId)(SubjectManager.slaveKey).last.value shouldBe empty
        subject.relations_history(masterId2)(SubjectManager.slaveKey) should have length 1
        subject.relations_history(masterId2)(SubjectManager.slaveKey).last.value shouldEqual List("0.5")
    }

    "Properties" should "be added and removed" in {
        val subject = TestData.subject
        val version = Version.apply("SM Test", Nil, sc, false, None)
        val sm = new SubjectManager(subject, version)

        sm.addProperties(Map("key 1" -> List("value 1", "value 2")))
        subject.properties("key 1") shouldEqual List("value 1", "value 2")
        subject.properties_history("key 1") should have length 1
        subject.properties_history("key 1").last.value shouldEqual List("value 1", "value 2")

        sm.addProperties(Map("key 2" -> List("value 3")))
        subject.properties("key 2") shouldEqual List("value 3")
        subject.properties_history("key 2") should have length 1
        subject.properties_history("key 2").last.value shouldEqual List("value 3")

        sm.addProperties(Map("key 2" -> List("value 4")))
        subject.properties("key 2") shouldEqual List("value 3", "value 4")
        subject.properties_history("key 2") should have length 2
        subject.properties_history("key 2").last.value shouldEqual List("value 3", "value 4")

        sm.addProperties(Map("key 3" -> List("value 5", "value 6"), "key 4" -> Nil))
        subject.properties("key 3") shouldEqual List("value 5", "value 6")
        subject.properties_history("key 3") should have length 1
        subject.properties_history("key 3").last.value shouldEqual List("value 5", "value 6")
        subject.properties.get("key 4") shouldBe empty
        subject.properties_history.get("key 4") shouldBe empty

        sm.overwriteProperties(Map("key 2" -> List("value 3")))
        subject.properties("key 2") shouldEqual List("value 3")
        subject.properties_history("key 2") should have length 3
        subject.properties_history("key 2").last.value shouldEqual List("value 3")

        sm.overwriteProperties(Map("key 2" -> Nil))
        subject.properties.get("key 2") shouldEqual None
        subject.properties_history("key 2") should have length 4
        subject.properties_history("key 2").last.value shouldBe empty

        sm.removeProperties(List("key 1"))
        subject.properties.get("key 1") shouldBe empty
        subject.properties_history("key 1") should have length 2
        subject.properties_history("key 1").last.value shouldBe empty

        sm.clearProperties()
        subject.properties.get("key 1") shouldBe empty
        subject.properties.get("key 2") shouldBe empty
        subject.properties.get("key 3") shouldBe empty
        subject.properties_history("key 1") should have length 2
        subject.properties_history("key 2") should have length 4
        subject.properties_history("key 3") should have length 2
        subject.properties_history("key 1").last.value shouldBe empty
        subject.properties_history("key 2").last.value shouldBe empty
        subject.properties_history("key 3").last.value shouldBe empty
    }

    "Relations" should "be added and removed" in {
        val subject = TestData.subject
        val version = Version.apply("SM Test", Nil, sc, false, None)
        val List(sub1, sub2) = TestData.masterIds()
        val sm = new SubjectManager(subject, version)

        sm.addRelations(Map(sub1 -> Map("key 1" -> "value 1", "key 2" -> "value 2")))
        subject.relations(sub1) shouldEqual Map("key 1" -> "value 1", "key 2" -> "value 2")
        subject.relations_history(sub1)("key 1") should have length 1
        subject.relations_history(sub1)("key 1").last.value shouldEqual List("value 1")
        subject.relations_history(sub1)("key 2") should have length 1
        subject.relations_history(sub1)("key 2").last.value shouldEqual List("value 2")

        sm.addRelations(Map(sub2 -> Map("key 3" -> "value 3")))
        subject.relations(sub2) shouldEqual Map("key 3" -> "value 3")
        subject.relations_history(sub2)("key 3") should have length 1
        subject.relations_history(sub2)("key 3").last.value shouldEqual List("value 3")

        sm.removeRelations(Map(sub1 -> List("key 1")))
        subject.relations(sub1) shouldEqual Map("key 2" -> "value 2")
        subject.relations_history(sub1)("key 1") should have length 2
        subject.relations_history(sub1)("key 1").last.value shouldBe empty

        sm.addRelations(Map(sub2 -> Map("key 4" -> "value 4")))
        subject.relations(sub2) shouldEqual Map("key 3" -> "value 3", "key 4" -> "value 4")
        subject.relations_history(sub2)("key 4") should have length 1
        subject.relations_history(sub2)("key 4").last.value shouldEqual List("value 4")

        sm.removeRelations(List(sub2))
        subject.relations.get(sub2) shouldBe empty
        subject.relations_history(sub2)("key 3") should have length 2
        subject.relations_history(sub2)("key 3").last.value shouldBe empty
        subject.relations_history(sub2)("key 4") should have length 2
        subject.relations_history(sub2)("key 4").last.value shouldBe empty

        sm.clearRelations()
        subject.relations_history(sub1)("key 1") should have length 2
        subject.relations_history(sub1)("key 2") should have length 2
        subject.relations_history(sub1)("key 2").last.value shouldBe empty
        subject.relations_history(sub2)("key 3") should have length 2
        subject.relations_history(sub2)("key 4") should have length 2
    }

    "The correct version" should "be found" in {
        val sm = new SubjectManager(Subject.empty(datasource = "testSource"), Version(program = ""))
        val foundVersions = TestData.versionQueries().map((sm.findVersion _).tupled).map(_.map(_.version))
        val expectedVersions = TestData.versionQueryResults()
        foundVersions shouldEqual expectedVersions
    }
}
