package de.hpi.ingestion.deduplication.models

import java.util.UUID

/**
  * Case class for corresponding duplicatecandidates cassandra table.
  * @param id unique UUID used as primary key
  * @param subject_id UUID of subject in the existing subject table
  * @param subject_name name of subject in the existing subject table
  * @param duplicate_id UUID of duplicate subject
  * @param duplicate_name name of duplicate subject
  * @param duplicate_table source table of duplicate
  */
case class DuplicateCandidate(
	id: UUID,
	subject_id: UUID,
	subject_name: Option[String] = None,
	duplicate_id: UUID,
	duplicate_name: Option[String] = None,
	duplicate_table: String)
