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
import de.hpi.ingestion.curation.{TestData => CTestData}
import de.hpi.ingestion.framework.CommandLineConf
import org.scalatest.{FlatSpec, Matchers}

class MasterUpdateTest extends FlatSpec with Matchers with SharedSparkContext {
    "Master nodes" should "be updated" in {
        val job = new MasterUpdate
        job.subjects = sc.parallelize(TestData.outdatedMasters())
        job.run(sc)
        val updatedMasters = job.updatedMasters.collect().toList.sortBy(_.id)
        val expectedMasters = TestData.updatedMasters().sortBy(_.id)
        updatedMasters should have length expectedMasters.length
        updatedMasters.zip(expectedMasters).foreach { case (master, expectedMaster) =>
            master.id shouldEqual expectedMaster.id
            master.name shouldEqual expectedMaster.name
            master.aliases shouldEqual expectedMaster.aliases
            master.category shouldEqual expectedMaster.category
            master.properties shouldEqual expectedMaster.properties
            master.relations shouldEqual expectedMaster.relations
        }
    }

    "Subjects that need to be updated" should "be extracted" in {
        val job = new MasterUpdate
        job.conf = CommandLineConf(Seq("-j", CTestData.commitJSON))
        job.subjects = sc.parallelize(TestData.commitSubjects)
        val updateSubjects = job.updateSubjects()
            .collect
            .map(subject => (subject.id, subject.master, subject.datasource))
            .toSet
        val expectedSubjects = TestData.commitUpdateSubjects
            .map(subject => (subject.id, subject.master, subject.datasource))
            .toSet
        updateSubjects shouldEqual expectedSubjects
    }

    "Master ids" should "be obtained when parsing the Commit JSON" in {
        val job = new MasterUpdate
        val masterIds = job.getMastersFromCommit(TestData.commitJSON)
        val expectedIds = TestData.masterIds
        masterIds shouldEqual expectedIds
    }
}
