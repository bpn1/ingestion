package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class CooccurrenceExportTest extends FlatSpec with Matchers with SharedSparkContext {

	"Nodes and Edges" should "be extracted" in {
		val input = List(sc.parallelize(TestData.exportCooccurrences())).toAnyRDD()
		val List(nodes, edges) = CooccurrenceExport.run(input, sc).fromAnyRDD[String]().map(_.collect.toSet)
		val expectedNodes = TestData.cooccurrenceNodes()
		val expectedEdges = TestData.cooccurrenceEdges()
		nodes shouldEqual expectedNodes
		edges shouldEqual expectedEdges
	}
}
