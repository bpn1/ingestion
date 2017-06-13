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
		val expectedNodeCSV = TestData.exportedNodeCSV.toSet
		val expectedEdgeCSV = TestData.exportedEdgeCSV.toSet
		nodes shouldEqual expectedNodeCSV
		edges shouldEqual expectedEdgeCSV
	}
}
