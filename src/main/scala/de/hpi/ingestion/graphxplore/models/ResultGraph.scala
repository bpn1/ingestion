package de.hpi.ingestion.graphxplore.models

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
