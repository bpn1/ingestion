package de.hpi.ingestion.deduplication

import de.hpi.ingestion.datalake.models.Subject

/**
  * This trait is the template for our blocking schemes.
  */
trait BlockingScheme extends Serializable {
	val undefinedValue = List("undefined")

	/**
	  * Generates key from the subject's properties.
	  * @param subject Subject to use.
	  * @return Key as list of strings.
	  */
	def generateKey(subject: Subject): List[String]
}

/**
  * This class uses the first three characters of the name property as key.
  */
class SimpleBlockingScheme extends BlockingScheme {
	override def generateKey(subject: Subject): List[String] = {
		if(subject.name.isDefined) {
			val name = subject.name.get
			List(name.substring(0, Math.min(3, name.length)))
		} else {
			undefinedValue
		}
	}
}

/**
  * This class uses a list of input attributes as key.
  */
class ListBlockingScheme extends BlockingScheme {
	/**
	  * This variable contains a list of attributes set by setAttributes.
	  */
	private var inputAttributes: List[String] = List[String]()

	/**
	  * Gets the attributes to generate the key with as input and converts them to a list.
	  * @param attrList Sequence of attributes to use.
	  * @return List of attributes as list of strings.
	  */
	def setAttributes(attrList: String*): Unit = inputAttributes = attrList.toList

	override def generateKey(subject: Subject): List[String] = {
		val key = inputAttributes.flatMap(subject.get)
		if (key.isEmpty) {
			undefinedValue
		} else {
			key
		}
	}
}
