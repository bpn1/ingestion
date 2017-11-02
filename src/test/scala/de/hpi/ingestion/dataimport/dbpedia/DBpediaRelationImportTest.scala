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

package de.hpi.ingestion.dataimport.dbpedia

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.datalake.models.Subject
import org.scalatest.{FlatSpec, Matchers}

class DBpediaRelationImportTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {
    "Relations" should "be added" in {
        val job = new DBpediaRelationImport
        job.subjects = sc.parallelize(TestData.dbpedia)
        job.relations = sc.parallelize(TestData.dbpediaRelations)
        job.run(sc)
        val output = job.subjectsWithRelations.collect.toList.sortBy(_.id)
        val expected =  TestData.dbpediaImportedRelations.sortBy(_.id)

        output should have length expected.length
        (output, expected).zipped.foreach { case (subject, expectedSubject) =>
            subject.relations shouldEqual expectedSubject.relations
        }
    }
}
