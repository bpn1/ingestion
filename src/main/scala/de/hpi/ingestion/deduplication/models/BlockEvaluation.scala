package de.hpi.ingestion.deduplication.models

import java.util.UUID

/**
  * Case class for corresponding block_evaluation cassandra table
  * @param id      unique UUID used as primary key
  * @param data    Map containing the information about the blocks sizes
  * @param comment String containing a comment or description of the data
  */
case class BlockEvaluation(
	id: UUID = UUID.randomUUID(),
	data: Map[List[String], Int] = Map[List[String], Int](),
	comment: Option[String] = None)
