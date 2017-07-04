package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class CooccurrenceExportTest extends FlatSpec with Matchers with SharedSparkContext {

	"Nodes and Edges" should "be extracted" in {
		val cooccurrences = sc.parallelize(TestData.exportCooccurrences())
		val dbpediaRelations = sc.parallelize(TestData.exportDBpediaRelations())
		val input = List(cooccurrences).toAnyRDD() ++ List(dbpediaRelations).toAnyRDD()
		val List(nodes, edges) = CooccurrenceExport.run(input, sc)
			.fromAnyRDD[String]()
			.map(_.collect.toSet)
		val expectedNodes = TestData.nodes()
		val expectedEdges = TestData.edges()
		nodes shouldEqual expectedNodes
		edges shouldEqual expectedEdges
	}
}
