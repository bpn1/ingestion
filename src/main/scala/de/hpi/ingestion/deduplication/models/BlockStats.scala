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
) {
	/**
	  * Assesses, depending on the number of calculations and a certain threshold, whether the block is too small
	  * to be written in the blocking evaluation job.
	  * @param minBlockSize The threshold
	  * @return whether the block is very small or not
	  */
	def isTiny(minBlockSize: Int): Boolean = {
		(this.numsubjects * this.numstaging) < minBlockSize
	}
}
