package de.hpi.ingestion.textmining.models

import java.util.UUID

import com.datastax.driver.core.utils.UUIDs
import de.hpi.ingestion.deduplication.models.PrecisionRecallDataTuple

/**
  * Contains statistics about a relation classifier.
  * @param id unique id used as primary key
  * @param rel relation that was classified
  * @param sentenceswithrelation number of sentences that contain the classified relation
  * @param sentenceswithnorelation number of sentences that don't contain the classified relation
  * @param average contains the average precision, recall and f-score
  * @param data contains all precision, recall and f-score data (multiple when cross-validating)
  * @param comment described this data entry
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
