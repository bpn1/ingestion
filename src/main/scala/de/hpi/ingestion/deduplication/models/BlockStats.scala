package de.hpi.ingestion.deduplication.models

case class BlockStats(
	key: String,
	numleft: Int,
	numright: Int
)
