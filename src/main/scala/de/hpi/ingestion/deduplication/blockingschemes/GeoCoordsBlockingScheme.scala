package de.hpi.ingestion.deduplication.blockingschemes

import de.hpi.ingestion.datalake.models.Subject

/**
  * This class uses a list of input attributes as key.
  */
class GeoCoordsBlockingScheme extends BlockingScheme {
	tag = "GeoCoordsBlockingScheme"
	override def generateKey(subject: Subject): List[String] = {
		val key = subject.get("geo_coords").grouped(2).map(_.mkString(";")).toList
		if (key.nonEmpty) key else List(undefinedValue)
	}
}

/**
  * Companion object adding an easy to use constructor via apply.
  */
object GeoCoordsBlockingScheme {

	/**
	  * Returns a geoCoords blocking scheme with the given tag and attributes.
	  * @param tag tag to use
	  * @return List Blocking Schemes with the given properties
	  */
	def apply(tag: String): GeoCoordsBlockingScheme = {
		val scheme = new GeoCoordsBlockingScheme
		scheme.tag = tag
		scheme
	}
}
