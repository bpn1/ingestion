package de.hpi.ingestion.deduplication.models

import java.util.UUID
import de.hpi.ingestion.datalake.models.Subject

case class Block(
	id: UUID = UUID.randomUUID(),
	key: String,
	subjects: List[Subject] = Nil,
	staging: List[Subject] = Nil
) {
	def evaluate(): BlockStats = BlockStats(key, subjects.length, staging.length)
}
