package de.hpi.ingestion.graphxplore.models

import java.util.UUID

case class ResultGraph (
	id: UUID,
	graphtype: String,
	nodes: List[UUID],
	size: Int
)

object ResultGraph {
	def apply(graphType: String, nodes: List[UUID]): ResultGraph = {
		ResultGraph(UUID.randomUUID(), graphType, nodes, nodes.length)
	}
}