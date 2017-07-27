package de.hpi.ingestion.graphxplore.models

import java.util.UUID

case class ResultGraph (
	id: UUID,
	graphtype: String,
	nodes: List[UUID],
	nodenames: List[String],
	size: Int
)

object ResultGraph {
	def apply(graphType: String, nodes: List[UUID], nodeNames: List[String]): ResultGraph = {
		ResultGraph(UUID.randomUUID(), graphType, nodes, nodeNames, nodes.length)
	}
}
