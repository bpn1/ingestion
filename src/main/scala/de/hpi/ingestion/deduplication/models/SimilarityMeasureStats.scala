package de.hpi.ingestion.deduplication.models

import java.util.UUID

case class SimilarityMeasureStats(
	id: UUID = UUID.randomUUID(),
	threshold: List[Double],
	precision: List[Double],
	f1Score: List[Double],
	comment: Option[String] = None
)
