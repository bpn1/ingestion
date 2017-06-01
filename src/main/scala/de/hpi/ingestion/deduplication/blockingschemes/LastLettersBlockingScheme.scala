package de.hpi.ingestion.deduplication.blockingschemes

import de.hpi.ingestion.datalake.models.Subject

/**
  * This class uses the last five characters of the name property as key.
  */
class LastLettersBlockingScheme extends BlockingScheme {
	tag = "LastLettersBlockingScheme"
	override def generateKey(subject: Subject): List[String] = {
		subject.name.map { name =>
			val start = Math.max(0, name.length - 5)
			List(name.substring(start, name.length))
		}.getOrElse(List(undefinedValue))
	}
}

/**
  * Companion object adding an easy to use constructor via apply.
  */
object LastLettersBlockingScheme {

	/**
	  * Returns a Last Letters Blocking Scheme with the given tag.
	  * @param tag tag to use
	  * @return Last Letters Blocking Schemes with the given tag
	  */
	def apply(tag: String = "LastLettersBlockingScheme"): LastLettersBlockingScheme = {
		val scheme = new LastLettersBlockingScheme
		scheme.tag = tag
		scheme
	}
}
