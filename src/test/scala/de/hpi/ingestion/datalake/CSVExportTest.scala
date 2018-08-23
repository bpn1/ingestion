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
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class CSVExportTest extends FlatSpec with Matchers with SharedSparkContext {
    "Relations" should "be converted to csv edges" in {
        val subjectEdges = CSVExportTestData.subjects.flatMap(CSVExport.edgesToCSV)
        val expectedCSV = CSVExportTestData.subjectEdgeCSV
        subjectEdges shouldEqual expectedCSV
    }

    "Subjects" should "be converted to csv nodes" in {
        val subjectNodes = CSVExportTestData.subjects.map(CSVExport.nodeToCSV)
        val expectedCSV = CSVExportTestData.subjectNodeCSV
        subjectNodes shouldEqual expectedCSV
    }

    they should "be parsed to nodes and edges" in {
        val job = new CSVExport
        job.subjects = sc.parallelize(CSVExportTestData.subjects)
        job.run(sc)
        val nodes = job.nodes.collect.toSet
        val edges = job.edges.collect.toSet
        val expectedNodeCSV = CSVExportTestData.masterNodeCSV.toSet
        val expectedEdgeCSV = CSVExportTestData.masterEdgeCSV.toSet
        nodes shouldEqual expectedNodeCSV
        edges shouldEqual expectedEdgeCSV
    }

    "Subject properties" should "be parsed to CSV" in {
        val subjects = CSVExportTestData.subjects
        val properties = subjects.map(CSVExport.propertiesToCSV)
        val expectedProperties = CSVExportTestData.propertyCSV
        properties shouldEqual expectedProperties
    }
}
