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

package de.hpi.ingestion.textmining.re

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.textmining.TestData
import org.scalatest.{FlatSpec, Matchers}

class CooccurrenceExportTest extends FlatSpec with Matchers with SharedSparkContext {

    "Nodes and Edges" should "be extracted" in {
        val job = new CooccurrenceExport
        job.cooccurrences = sc.parallelize(TestData.exportCooccurrences())
        job.relations = sc.parallelize(TestData.exportDBpediaRelations())
        job.run(sc)
        val List(nodes, edges) = List(job.nodes, job.edges).map(_.collect.toSet)
        val expectedNodes = TestData.nodes()
        val expectedEdges = TestData.edges()
        nodes shouldEqual expectedNodes
        edges shouldEqual expectedEdges
    }
}
