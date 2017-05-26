package de.hpi.ingestion.deduplication.models

import java.util.UUID
import com.datastax.driver.core.utils.UUIDs

/**
  * Case class for corresponding sim_measure_stats cassandra table
  * @param id unique UUID used as primary key
  * @param data Array containing (threshold, precision, recall, f1Score) tuple
  * @param comment String containing a comment or description of the data
  */
case class SimilarityMeasureStats(
	id: UUID = UUIDs.timeBased(),
	data: List[PrecisionRecallDataTuple] = Nil,
	comment: Option[String] = None
)
