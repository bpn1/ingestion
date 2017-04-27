package de.hpi.ingestion.deduplication

import de.hpi.ingestion.datalake.models.Subject

/**
  * This trait is the template for our blocking schemes.
  */
trait BlockingScheme extends Serializable {
	val undefinedValue = List("undefined")
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
		}.getOrElse(undefinedValue)
	}
}

/**
  * This class uses a list of input attributes as key.
  */
class ListBlockingScheme extends BlockingScheme {
	tag = "ListBlockingScheme"
	override def generateKey(subject: Subject): List[String] = {
		val key = inputAttributes.flatMap(subject.get)
		if (key.nonEmpty) key else undefinedValue
	}
}

/**
  * This class uses geo coordinates as a key
  */
class GeoBlockingScheme extends BlockingScheme {
	this.setAttributes("geo_coords")
	def calculateRange(lat: Double, long: Double, scale: Double = 1.0): List[Int] = {
		List((lat / scale).toInt, (long / scale).toInt)
	}

	override def generateKey(subject: Subject): List[String] = {
		val key = inputAttributes.flatMap { attribute =>
			subject.get(attribute).sliding(2,2).toList.map {
				case lat::long::Nil => calculateRange(lat.toDouble, long.toDouble).map(_.toString).mkString(",")
				case _ => undefinedValue.head
			}.distinct
		}
		if (key.nonEmpty) key else undefinedValue
	}
}
