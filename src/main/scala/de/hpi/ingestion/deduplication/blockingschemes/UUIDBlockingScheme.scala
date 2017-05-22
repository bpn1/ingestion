package de.hpi.ingestion.deduplication.blockingschemes

import de.hpi.ingestion.datalake.models.Subject

/**
  * This class uses a modulo function to generate a random key.
  */
class UUIDBlockingScheme extends BlockingScheme {
	tag = "UUIDBlockingScheme"
	override def generateKey(subject: Subject): List[String] = {
		List((subject.get("id").hashCode % 1000).toString)
	}
}

/**
  * Companion object adding an easy to use constructor via apply.
  */
object UUIDBlockingScheme {

	/**
	  * Returns a (random) blocking scheme over UUIDs with the given tag.
	  * @param tag tag to use
	  * @return Simple Blocking Schemes with the given tag
	  */
	def apply(tag: String = "UUIDBlockingScheme"): UUIDBlockingScheme = {
		val scheme = new UUIDBlockingScheme
		scheme.tag = tag
		scheme
	}
}
