package de.hpi.ingestion.deduplication.blockingschemes

import de.hpi.ingestion.datalake.models.Subject

/**
  * This class uses the first five characters of the name property as key, ignoring "The ".
  */
class SimpleBlockingScheme extends BlockingScheme {
	tag = "SimpleBlockingScheme"
	override def generateKey(subject: Subject): List[String] = {
		subject.name.map { name =>
			if (name.startsWith("The ")) {
				List(name.substring(4, Math.min(9, name.length)))
			} else {
				List(name.substring(0, Math.min(5, name.length)))
			}
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
