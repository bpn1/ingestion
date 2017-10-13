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

class ConnectedComponents extends GraphExtractor {
	appName = "ConnectedComponents"
	val graphType = "ConnectedComponent"
	val minComponentSize = 3

	/**
	  * Finds connected components in the given graph
	  * @param graph GraphFrame that is processed
	  * @return RDD of ResultGraph objects that represent extracted connected components
	  */
	def findConnectedComponents(graph: GraphFrame): RDD[ResultGraph] = {
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

	override def processGraph(graph: GraphFrame): RDD[ResultGraph] = {
		findConnectedComponents(graph)
	}
}
