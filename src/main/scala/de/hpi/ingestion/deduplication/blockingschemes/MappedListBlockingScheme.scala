package de.hpi.ingestion.deduplication.blockingschemes

import de.hpi.ingestion.datalake.models.Subject

/**
  * This class uses a list of attributes as key.
  * @param f Function to create customized output values
  */
class MappedListBlockingScheme(f: String => String = identity) extends BlockingScheme {
	tag = "ListBlockingScheme"
	override def generateKey(subject: Subject): List[String] = {
		val key = inputAttributes.flatMap(subject.get(_).map(f))
		if(key.nonEmpty) key else List(undefinedValue)
	}
}

/**
  * Companion object adding an easy to use constructor via apply.
  */
object MappedListBlockingScheme {

	/**
	  * Returns a Mapped List Blocking Scheme with the given tag, function and attributes.
	  * @param tag tag to use
	  * @param f function to use
	  * @param attrList attribute list to use
	  * @return Mapped List Blocking Schemes with the given properties
	  */
	def apply(tag: String, f: String => String, attrList: String*): MappedListBlockingScheme = {
		val scheme = new MappedListBlockingScheme(f)
		scheme.tag = tag
		scheme.inputAttributes = attrList.toList
		scheme
	}
}
