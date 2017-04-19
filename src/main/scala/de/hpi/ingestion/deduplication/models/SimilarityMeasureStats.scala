package de.hpi.ingestion.deduplication.models

import java.util.UUID

case class Metrics(
	precision: Double = 0.0,
	recall: Double = 0.0,
	f_score: Double = 0.0
)

case class SimilarityMeasureStats(
	id: UUID = UUID.randomUUID(),
	data: Map[String, List[(Metrics, Double)]],
	comment: Option[String] = None
)
