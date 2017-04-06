package de.hpi.ingestion.dataimport.dbpedia.models

import java.util.UUID

case class DuplicateCandidates(
	id: UUID = UUID.randomUUID(),
	subject: UUID,
	candidates: Option[List[(UUID, UUID, String)]]
){
	override def hashCode(): Int = (this.subject, this.candidates).hashCode()

	override def equals(obj: Any): Boolean = obj match {
		case that: DuplicateCandidates => that.subject == this.subject && that.candidates == this.candidates
	}
}
