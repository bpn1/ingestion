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

	"Subjects" should "be converted to csv nodes" in {
		val subjectNodes = TestData.exportSubjects.map(CSVExport.nodeToCSV)
		val expectedCSV = TestData.exportedNodeCSV
		subjectNodes shouldEqual expectedCSV
	}

	"Relations" should "be converted to csv edges" in {
		val subjectEdges = TestData.exportSubjects.flatMap(CSVExport.edgesToCSV)
		val expectedCSV = TestData.exportedEdgeCSV
		subjectEdges shouldEqual expectedCSV
	}

	"Subjects" should "be parsed to nodes and edges" in {
		val input = List(sc.parallelize(TestData.exportSubjects)).toAnyRDD()
		val List(nodes, edges) = CSVExport.run(input, sc).fromAnyRDD[String]().map(_.collect.toSet)
		val expectedNodeCSV = TestData.exportedMasterNodeCSV.toSet
		val expectedEdgeCSV = TestData.exportedMasterEdgeCSV.toSet
		nodes shouldEqual expectedNodeCSV
		edges shouldEqual expectedEdgeCSV
	}
}
