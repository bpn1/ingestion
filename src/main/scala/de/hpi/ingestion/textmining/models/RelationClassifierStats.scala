package de.hpi.ingestion.deduplication.models

import java.util.UUID
import com.datastax.driver.core.utils.UUIDs

/**
  *
   * @param id
  * @param rel
  * @param sentenceswithrelation
  * @param sentenceswithnorelation
  * @param average
  * @param data
  * @param comment
  */
case class RelationClassifierStats(
	id: UUID = UUIDs.timeBased(),
	rel: String,
	sentenceswithrelation: Int,
	sentenceswithnorelation: Int,
	average: PrecisionRecallDataTuple,
	data: List[PrecisionRecallDataTuple] = Nil,
	comment: Option[String] = None
)
