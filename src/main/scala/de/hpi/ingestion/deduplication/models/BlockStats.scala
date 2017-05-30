package de.hpi.ingestion.deduplication.models

/**
  * Contains the statistical data of a single block.
  * @param key key used to generate this block
  * @param numsubjects number of Subjects in this block
  * @param numstaging number of staged Subjects in this block
  */
case class BlockStats(
	key: String,
	numsubjects: Int,
	numstaging: Int,
	precision: Double = 0.0
)
