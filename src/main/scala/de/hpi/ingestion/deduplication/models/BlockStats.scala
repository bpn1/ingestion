package de.hpi.ingestion.deduplication.models

case class BlockStats(
	key: String,
	subjects: Int = 0,
	staging: Int = 0
)
