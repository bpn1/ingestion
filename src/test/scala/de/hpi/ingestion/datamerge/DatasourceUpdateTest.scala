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

package de.hpi.ingestion.datamerge

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class DatasourceUpdateTest extends FlatSpec with Matchers with SharedSparkContext {
    "Subjects" should "be updated" in {
        val oldSubjects = TestData.subjectsToUpdate()
        val newSubjects = TestData.updateSubjects()
        val version = TestData.version
        val updatedSubjects = oldSubjects.zip(newSubjects).map {
            case (oldSubject, newSubject) => DatasourceUpdate.updateSubject(oldSubject, newSubject, version)
        }
        val expectedSubjects = TestData.updatedSubjects()
        updatedSubjects should have length expectedSubjects.length
        updatedSubjects.zip(expectedSubjects).foreach { case (subject, expectedSubject) =>
            subject.id shouldEqual expectedSubject.id
            subject.name shouldEqual expectedSubject.name
            subject.aliases shouldEqual expectedSubject.aliases
            subject.category shouldEqual expectedSubject.category
            subject.properties shouldEqual expectedSubject.properties
            subject.relations shouldEqual expectedSubject.relations
        }
    }

    "Old Subjects" should "be updated correctly and extended with new Subjects" in {
        val job = new DatasourceUpdate
        job.subjects = sc.parallelize(TestData.oldSubjects())
        job.subjectsWithUpdate = sc.parallelize(TestData.newSubjects())
        job.run(sc)
        val updatedSubjects = job.updatedSubjects.collect.toList.sortBy(_.id)
        val expectedSubjects = TestData.updatedAndNewSubjects().sortBy(_.id)
        updatedSubjects should have length expectedSubjects.length
        updatedSubjects.filter(_.isMaster) should have length 1
        updatedSubjects.filter(_.isSlave).zip(expectedSubjects.filter(_.isSlave)).foreach {
            case (subject, expectedSubject) =>
                subject.id shouldEqual expectedSubject.id
                subject.name shouldEqual expectedSubject.name
                subject.aliases shouldEqual expectedSubject.aliases
                subject.category shouldEqual expectedSubject.category
                subject.properties shouldEqual expectedSubject.properties
                subject.masterRelations shouldEqual expectedSubject.masterRelations
        }
    }
}
