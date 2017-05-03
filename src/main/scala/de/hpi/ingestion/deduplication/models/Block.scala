package de.hpi.ingestion.deduplication.models

import java.util.UUID
import de.hpi.ingestion.datalake.models.Subject

case class Blocking(
	id: UUID = UUID.randomUUID(),
	key: Option[String] = None,
	subjects: List[Subject] = Nil,
	staging: List[Subject] = Nil,
	comment: Option[String] = None
)
