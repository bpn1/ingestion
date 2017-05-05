package de.hpi.ingestion.deduplication.models

import java.util.UUID
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.implicits.CollectionImplicits._

case class Block(
	id: UUID = UUID.randomUUID(),
	key: String,
	subjects: List[Subject] = Nil,
	staging: List[Subject] = Nil
) {
	def evaluate(): BlockStats = BlockStats(key, subjects.length, staging.length)

	def crossProduct(): List[(Subject, Subject)] = subjects.cross(staging).toList

}
