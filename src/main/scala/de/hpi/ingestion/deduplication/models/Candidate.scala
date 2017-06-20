package de.hpi.ingestion.deduplication.models

import java.util.UUID

/**
  * Duplicate candidate
  * @param id UUID of the candidate
  * @param name name
  * @param score similarity score
  */
case class Candidate(
	id: UUID = UUID.randomUUID(),
	name: Option[String] = None,
	score: Double = 0.0
)
