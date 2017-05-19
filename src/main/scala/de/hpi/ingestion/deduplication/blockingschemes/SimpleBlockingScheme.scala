package de.hpi.ingestion.deduplication.blockingschemes

import de.hpi.ingestion.datalake.models.Subject

/**
  * This class uses the first three characters of the name property as key.
  */
class SimpleBlockingScheme extends BlockingScheme {
	tag = "SimpleBlockingScheme"
	override def generateKey(subject: Subject): List[String] = {
		subject.name.map { name =>
			List(name.substring(0, Math.min(3, name.length)))
		}.getOrElse(List(undefinedValue))
	}
}
/**
  * Companion object adding an easy to use constructor via apply.
  */
object SimpleBlockingScheme {

	/**
	  * Returns a Simple Blocking Scheme with the given tag.
	  * @param tag tag to use
	  * @return Simple Blocking Schemes with the given tag
	  */
	def apply(tag: String = "SimpleBlockingScheme"): SimpleBlockingScheme = {
		val scheme = new SimpleBlockingScheme
		scheme.tag = tag
		scheme
	}
}
