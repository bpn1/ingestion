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

package de.hpi.ingestion.graphframes

import java.util.UUID
import org.apache.spark.rdd.RDD
import org.graphframes.GraphFrame
import de.hpi.ingestion.graphframes.models.ResultGraph

/**
  * Extracts company groups in a graph by finding connected components in its ownership relations
  */
class CompanyGroupExtractor extends GraphExtractor {
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
	override def processGraph(graph: GraphFrame): RDD[ResultGraph] = {
		val ownershipEdges = graph.edges.filter((edge) =>
			ownershipRelations.contains(edge.getAs[String]("relationship")))
		val ownershipGraph = GraphFrame(graph.vertices, ownershipEdges)
		findCompanyGroups(ownershipGraph, graphType)
	}
}
