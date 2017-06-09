package de.hpi.ingestion.deduplication.blockingschemes

import de.hpi.ingestion.datalake.models.Subject

/**
  * This class uses a modulo function to generate a random key.
  */
class RandomBlockingScheme extends BlockingScheme {
	tag = "RandomBlockingScheme"
	override def generateKey(subject: Subject): List[String] = {
		List((subject.id.hashCode % 1000).toString)
	}
}

/**
  * Companion object adding an easy to use constructor via apply.
  */
object RandomBlockingScheme {

	/**
	  * Returns a (random) blocking scheme over UUIDs with the given tag.
	  * @param tag tag to use
	  * @return Simple Blocking Schemes with the given tag
	  */
	def apply(tag: String = "UUIDBlockingScheme"): RandomBlockingScheme = {
		val scheme = new RandomBlockingScheme
		scheme.tag = tag
		scheme
	}
}
