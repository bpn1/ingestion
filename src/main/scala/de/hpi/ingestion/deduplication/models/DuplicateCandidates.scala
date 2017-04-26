package de.hpi.ingestion.deduplication.models

import java.util.UUID
import de.hpi.ingestion.datalake.models.Subject

case class DuplicateCandidates(
	subject_id: UUID,
	candidates: List[(Subject, String, Double)] = List[(Subject, String, Double)]())
