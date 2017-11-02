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

package de.hpi.ingestion.datalake.models

import org.scalatest.{FlatSpec, Matchers}

class SubjectTest extends FlatSpec with Matchers {
    "Equality" should "only compare UUIDs of Subjects" in {
        val subject1 = Subject(id = TestData.idList.head, master = null, datasource = "test")
        val subject2 = Subject(id = TestData.idList.head, master = null, datasource = "test")
        val subject3 = Subject(id = TestData.idList(1), master = null, datasource = "test")

        subject1 shouldEqual subject2
        subject1 should not equal subject3
        subject1.equals(subject2.id) shouldBe false
    }

    "Attribute values" should "be returned" in {
        val id = TestData.idList.head
        val datasource = "test"
        val name = "name"
        val aliases = List("alias")
        val subject = Subject(id = null, master = id, datasource = datasource, name = Option(name), aliases = aliases)

        subject.get("id") shouldBe empty
        subject.get("master") shouldEqual List(id.toString)
        subject.get("datasource") shouldEqual List(datasource)
        subject.get("name") shouldEqual List(name)
        subject.get("category") shouldBe empty
        subject.get("aliases") shouldEqual aliases
        subject.get("properties") shouldBe empty
    }

    "Normalized properties" should "be returned" in {
        val subject = TestData.subject
        val properties = subject.normalizedProperties
        val expected = TestData.normalizedProperties
        properties shouldEqual expected
    }

    "masterScore" should "should return the score of the master relation" in {
        val slave = TestData.slave
        val master = TestData.master

        slave.masterScore should contain(0.5)
        master.masterScore shouldEqual None
    }

    "Master" should "be easily created" in {
        val master = Subject.master(TestData.idList.head)

        master.id shouldEqual TestData.idList.head
        master.master shouldEqual TestData.idList.head
        master.datasource shouldEqual "master"
    }

    "Empty Subject" should "be easily created" in {
        val emptySubject = Subject.empty(TestData.idList.head, "test")

        emptySubject.id shouldEqual TestData.idList.head
        emptySubject.master shouldEqual TestData.idList.head
        emptySubject.datasource shouldEqual "test"
    }

    "Subject" should "be transformed to tsv" in {
        val tsvSubjects = TestData.exportSubjects.map(_.toTsv)
        val expectedSubjects = TestData.tsvSubjects
        tsvSubjects shouldEqual expectedSubjects
    }
}
