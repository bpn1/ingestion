package de.hpi.ingestion.deduplication.models

import java.util.UUID

/**
  * Possible duplicates of subject
  * @param subject_id UUID of the subject
  * @param subject_name name
  * @param datasource source where the candidates come from
  * @param candidates List of duplicate candidates
  */
case class Duplicates(
	subject_id: UUID,
	subject_name: Option[String] = None,
	datasource: String,
	candidates: List[Candidate] = Nil
)
