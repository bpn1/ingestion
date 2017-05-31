package de.hpi.ingestion.deduplication.blockingschemes

import de.hpi.ingestion.datalake.models.Subject

/**
  * This trait is the template for our blocking schemes.
  */
trait BlockingScheme extends Serializable {
	val undefinedValue = "undefined"
	var tag = "BlockingScheme"
	var inputAttributes: List[String] = List[String]()

	/**
	  * Gets the attributes to generate the key with as input and converts them to a list
	  * if required for the blocking scheme.
	  * @param attrList Sequence of attributes to use.
	  */
	def setAttributes(attrList: String*): Unit = inputAttributes = attrList.toList

	/**
	  * Generates key from the subject's properties.
	  * @param subject Subject to use.
	  * @return Key as list of strings.
	  */
	def generateKey(subject: Subject): List[String]
}
