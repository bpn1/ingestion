package de.hpi.ingestion.graphxplore
import java.util.UUID

import de.hpi.ingestion.graphxplore.models.ResultGraph
import org.apache.spark.rdd.RDD
import org.graphframes.GraphFrame

object ConnectedComponents extends GraphExtractor {
	appName = "ConnectedComponents"
	val graphType = "ConnectedComponent"
	val minComponentSize = 3

	/**
	  * Finds connected components in the given graph
	  * @param graph GraphFrame that is processed
	  * @return RDD of ResultGraph objects that represent extracted connected components
	  */
	override def processGraph(graph: GraphFrame): RDD[ResultGraph] = {
		graph.connectedComponents.run()
			.select("id", "component", "name")
			.rdd
			.map(row => (
				row.getAs[Long]("component"),
				UUID.fromString(row.getAs[String]("id")),
				row.getAs[String]("name")))
			.groupBy(_._1) // component
			.map(_._2.map(t => (t._2, t._3)).toList) // extract id and name
			.filter(_.length >= minComponentSize)
			.map(idNameTuples => ResultGraph(
				graphType,
				idNameTuples.map(_._1),
				idNameTuples.filter(_._2 != null).map(_._2)))
	}
}
