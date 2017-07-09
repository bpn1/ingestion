package de.hpi.ingestion.graphxplore

import java.util.UUID

import de.hpi.ingestion.graphxplore.models.ResultGraph
import org.apache.spark.rdd.RDD
import org.graphframes.GraphFrame

object CompanyGroupExtractor extends GraphExtractor {
	appName = "CompanyGroupExtractor"
	val graphType = "CompanyGroup"
	val minComponentSize = 3

	val ownershipRelations = List(
		"ownedBy",
		"parentCompany",
		"parent organization",
		"subsidiary",
		"owner",
		"owningCompany",
		"location of formation" //?
	)

	// when operating on normalized edges
	/* val ownershipRelations = List(
		"owns",
		"owned by"
	) */

	/**
	  * Finds connected components based on the ownership relations in the given graph
	  * @param graph GraphFrame that is processed
	  * @return RDD of ResultGraph objects that represent extracted company groups
	  */
	override def processGraph(graph: GraphFrame): RDD[ResultGraph] = {
		// pre-filter for ownership-related edges
		val ownershipEdges = graph.edges.filter((edge) =>
			ownershipRelations.contains(edge.getAs[String]("relationship")))
		val ownershipGraph = GraphFrame(graph.vertices, ownershipEdges)

		ownershipGraph.connectedComponents.run()
			.select("id", "component", "name")
			.rdd
			.map(row => (
				row.getAs[Long]("component"),
				UUID.fromString(row.getAs[String]("id")),
				row.getAs[String]("name")))
			.groupBy(_._1) // component
			.map(_._2.map(t => (t._2, t._3)).toList) // extract id and name
			.filter(_.length >= minComponentSize)
			.map(idList => ResultGraph(graphType, idList.map(_._1), idList.filter(_._2 != null).map(_._2)))
	}
}
