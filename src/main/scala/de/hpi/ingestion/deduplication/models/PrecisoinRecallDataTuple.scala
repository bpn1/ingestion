package de.hpi.ingestion.deduplication.models

/**
  * A Data Tuple to hold the values for given argument in precision, recall and fscore
  * @param argument
  * @param precision
  * @param recall
  * @param fscore
  */
case class PrecisionRecallDataTuple(
	argument: Double,
	precision: Double,
	recall: Double,
	fscore: Double
)
