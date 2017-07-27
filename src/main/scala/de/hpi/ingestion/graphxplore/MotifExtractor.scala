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

package de.hpi.ingestion.graphxplore

import java.util.UUID
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.graphframes.GraphFrame
import de.hpi.ingestion.graphxplore.models.ResultGraph

/**
  * Extracts star and chain structures from subject graph
  */
object MotifExtractor extends GraphExtractor {
	appName = "MotifExtractor"
	/**
	  * Minimal size for star motifs
	  */
	val minStarSize = 3
	/**
	  * Maximal size for star motifs. Can't be > 26, because single chars are used for node identification
	  */
	val maxStarSize = 10

	/**
	  * Searches for a given motif query in the provided graph
	  * @param graph Graph thas is to be searched
	  * @param query String for the graph query (relations that should exist between returned nodes)
	  * @param nodeNames Names of the nodes contained in the query
	  * @param graphType Type that should be given to the returned ResultGraph
	  * @return ResultGraph instance containing the found nodes
	  */
	def findMotifs(graph: GraphFrame, query: String, nodeNames: List[String], graphType: String): RDD[ResultGraph] = {
		val motifs = graph.find(query)

		val idColumns = nodeNames.map(name => col(name.toString + ".id"))
		val nameColumns = nodeNames.map(name => col(name.toString + ".name"))

		motifs
			.select(idColumns ++ nameColumns: _*)
			.rdd
			.map(_.toSeq.toList)
			.filter(_.forall(_ != null))
			.map(_.map(_.toString))
			.map(columns => {
				val ids = columns.take(nodeNames.size).map(UUID.fromString)
				val names = columns.slice(nodeNames.size, columns.size)
				ids.zip(names)
			})
			.map(idNameTuples => ResultGraph(
				graphType,
				idNameTuples.map(_._1),
				idNameTuples.filter(_._2 != null).map(_._2)))
	}

	/**
	  * Finds pre-defined motifs in the given graph
	  * @param graph GraphFrame that is processed
	  * @return RDD of ResultGraph objects that represent extracted company groups
	  */
	override def processGraph(graph: GraphFrame): List[RDD[ResultGraph]] = {
		var resultGraphs = List[RDD[ResultGraph]]()

		for(starSize <- minStarSize to maxStarSize) {
			val nodeNames = ('a' to 'z').take(starSize).map(_.toString).toList :+ "center"
			val motifQuery = nodeNames.map(char => s"($char)-[]->(center)").mkString("; ")
			resultGraphs :+= findMotifs(graph, motifQuery, nodeNames, "Star")
		}

		val chainQuery = "(a)-[]->(b); (b)-[]->(c); (c)-[]->(d)"
		val chainNames = List("a", "b", "c", "d")
		resultGraphs :+= findMotifs(graph, chainQuery, chainNames, "Chain")

		resultGraphs
	}
}
