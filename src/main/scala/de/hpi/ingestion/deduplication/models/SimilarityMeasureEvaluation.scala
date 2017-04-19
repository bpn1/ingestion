package de.hpi.ingestion.deduplication.models

import java.util.UUID

case class SimilariyMeasureStats(
	precision: Double = 0.0,
	recall: Double = 0.0,
	f_score: Double = 0.0
)

case class SimilarityMeasureEvaluation(
	id: UUID = UUID.randomUUID(),
	data: Map[String, List[(SimilariyMeasureStats, Double)]],
	comment: Option[String] = None
)
