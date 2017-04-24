package de.hpi.ingestion.deduplication

import de.hpi.ingestion.datalake.models.Subject

trait ListComparer extends Serializable {
	def compare(attribute: String, subject1: Subject, subject2: Subject)

}
