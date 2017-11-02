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

package de.hpi.ingestion.graphframes.models

import java.util.UUID

/**
  * Contains an extracted subgraph of the original subject graph
  * @param id Unique identifier for this graph
  * @param graphtype Type of graph (can be used to differentiate different job's output)
  * @param nodes List of the UUIDs of the nodes contained in the graph
  * @param nodenames List of the names of the nodes contained in the graph
  * @param size Number of nodes in this graph
  */
case class ResultGraph (
    id: UUID,
    graphtype: String,
    nodes: List[UUID],
    nodenames: List[String],
    size: Int
)

/**
  * Companion object of ResultGraph case class
  */
object ResultGraph {
    /**
      * Alternative constructor for easier creation of ResultGraph instances
      * @param graphType Type of graph (can be used to differentiate different job's output)
      * @param nodes List of the UUIDs of the nodes contained in the graph
      * @param nodeNames List of the names of the nodes contained in the graph
      * @return ResultGraph instance that contains the given nodes data
      */
    def apply(graphType: String, nodes: List[UUID], nodeNames: List[String]): ResultGraph = {
        ResultGraph(UUID.randomUUID(), graphType, nodes, nodeNames, nodes.length)
    }
}
