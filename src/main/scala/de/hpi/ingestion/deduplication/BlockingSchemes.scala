package de.hpi.ingestion.deduplication

import de.hpi.ingestion.datalake.models.Subject

/**
  * This trait is the template for our blocking schemes.
  */
trait BlockingScheme extends Serializable {
	val undefinedValue = "undefined"
	var tag = "BlockingScheme"
	protected var inputAttributes: List[String] = List[String]()

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
  * This class uses a list of input attributes as key.
  */
class ListBlockingScheme extends BlockingScheme {
	tag = "ListBlockingScheme"
	override def generateKey(subject: Subject): List[String] = {
		val key = inputAttributes.flatMap(subject.get)
		if (key.nonEmpty) key else List(undefinedValue)
	}
}

/**
  * This class uses a list of attributes as key.
  * @param f Function to create customized output values
  */
class MappedListBlockingScheme(f: String => String = identity) extends BlockingScheme {
	tag = "ListBlockingScheme"
	override def generateKey(subject: Subject): List[String] = {
		val key = inputAttributes.flatMap(subject.get(_).map(f))
		if (key.nonEmpty) key else List(undefinedValue)
	}
}

/**
  * This class transforms two related coordinates to keys.
  */
class GeoCoordsBlockingScheme extends BlockingScheme {
	tag = "GeoCoordsBlockingScheme"
	override def generateKey(subject: Subject): List[String] = {
		val key = subject.get("geo_coords").grouped(2).map(_.mkString(",")).toList
		if (key.nonEmpty) key else List(undefinedValue)
	}
}
