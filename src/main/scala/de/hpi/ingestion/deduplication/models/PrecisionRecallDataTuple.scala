package de.hpi.ingestion.deduplication.models

/**
  * A Data Tuple to hold the values for given argument in precision, recall and fscore
  * @param threshold
  * @param precision
  * @param recall
  * @param fscore
  */
case class PrecisionRecallDataTuple(
	threshold: Double,
	precision: Double,
	recall: Double,
	fscore: Double
)
