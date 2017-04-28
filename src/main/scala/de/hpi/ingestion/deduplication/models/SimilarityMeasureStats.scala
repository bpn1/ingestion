package de.hpi.ingestion.deduplication.models

import java.util.UUID
import com.datastax.driver.core.utils.UUIDs
import org.apache.spark.rdd.RDD

/**
  * Case class for corresponding sim_measure_stats cassandra table
  * @param id unique UUID used as primary key
  * @param data Array containing (threshold, precision, recall, f1Score) tuple
  * @param comment String containing a comment or description of the data
  */
case class SimilarityMeasureStats(
	id: UUID = UUIDs.timeBased(),
	data: RDD[PrecisionRecallDataTuple],
	comment: Option[String]
)

