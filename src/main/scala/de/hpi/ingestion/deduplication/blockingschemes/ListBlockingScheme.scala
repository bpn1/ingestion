package de.hpi.ingestion.deduplication.blockingschemes

import de.hpi.ingestion.datalake.models.Subject

/**
  * This class uses a list of input attributes as key.
  */
class ListBlockingScheme extends BlockingScheme {
	tag = "ListBlockingScheme"
	override def generateKey(subject: Subject): List[String] = {
		val key = inputAttributes.flatMap(subject.get)
		if(key.nonEmpty) key else List(undefinedValue)
	}
}

/**
  * Companion object adding an easy to use constructor via apply.
  */
object ListBlockingScheme {

	/**
	  * Returns a List Blocking Scheme with the given tag and attributes.
	  * @param tag tag to use
	  * @param attrList attribute list to use
	  * @return List Blocking Schemes with the given properties
	  */
	def apply(tag: String, attrList: String*): ListBlockingScheme = {
		val scheme = new ListBlockingScheme
		scheme.tag = tag
		scheme.inputAttributes = attrList.toList
		scheme
	}
}
