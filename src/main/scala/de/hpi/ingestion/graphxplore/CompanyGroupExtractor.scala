package de.hpi.ingestion.graphxplore

import java.util.UUID

import de.hpi.ingestion.graphxplore.models.ResultGraph
import org.apache.spark.rdd.RDD
import org.graphframes.GraphFrame

/**
  * Extracts company groups in a graph by finding connected components in its ownership relations
  */
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
		"owningCompany"
	)

	/**
	  * Extract company group structures from the graph by searching for connected components
	  * @param graph Graph that is to be searched
	  * @param outputGraphType Type that is assigned to all returned ResultGraphs
	  * @return RDD of ResultGraph that contain the extracted company group nodes
	  */
	def findCompanyGroups(graph: GraphFrame, outputGraphType: String): RDD[ResultGraph] = {
		graph.connectedComponents.run()
			.join(graph.degrees, List("id"))
			.select("id", "component", "name", "degree")
			.rdd
			.map(row => {
				val component = row.getAs[Long]("component")
				val id = UUID.fromString(row.getAs[String]("id"))
				val name = row.getAs[String]("name")
				val degree = row.getAs[Int]("degree")
				(component, id, name, degree)
			})
			.groupBy(_._1)
			.map(_._2.map { case (component, id, name, degree) => (id, name, degree) }.toList)
			.filter(_.length >= minComponentSize)
			.map(_.sortBy(_._3)(Ordering[Int].reverse))
			.map(idNameTuples => {
				val ids = idNameTuples.map(_._1)
				val names = idNameTuples.filter(_._2 != null).map(_._2)
				ResultGraph(outputGraphType, ids, names)
			})
	}

	/**
	  * Finds connected components based on the ownership-related relations in the given graph
	  * @param graph GraphFrame that is processed
	  * @return RDD of ResultGraph objects that represent extracted company groups
	  */
	override def processGraph(graph: GraphFrame): List[RDD[ResultGraph]] = {
		val ownershipEdges = graph.edges.filter((edge) =>
			ownershipRelations.contains(edge.getAs[String]("relationship")))
		val ownershipGraph = GraphFrame(graph.vertices, ownershipEdges)

		List(findCompanyGroups(ownershipGraph, graphType))
	}
}
