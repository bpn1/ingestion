package de.hpi.ingestion.deduplication.blockingschemes

import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.implicits.CollectionImplicits._

/**
  * This class uses geo coordinates as key.
  */
class GeoCoordsBlockingScheme extends BlockingScheme {
	tag = "GeoCoordsBlockingScheme"
	override def generateKey(subject: Subject): List[String] = {
		val coords = subject.get("geo_coords")
		if (coords.nonEmpty) {
			coords.flatMap { coord =>
				val Array(lat, long) = coord.split(";").map(_.toDouble)
				val lat1 = (math floor lat * 10) / 10
				val long1 = (math floor long * 10) / 10
				val lat2 = (math floor lat * 10 + 1) / 10
				val long2 = (math floor long * 10 + 1) / 10
				val keys = List(lat1, lat2).cross(List(long1, long2))
				keys.map(_.productIterator.mkString(";"))
			}
		} else { List(undefinedValue) }
	}
}

/**
  * Companion object adding an easy to use constructor via apply.
  */
object GeoCoordsBlockingScheme {

	/**
	  * Returns a geo coords Blocking Scheme with the given tag.
	  * @param tag tag to use
	  * @return geo coords blocking scheme with the given tag
	  */
	def apply(tag: String): GeoCoordsBlockingScheme = {
		val scheme = new GeoCoordsBlockingScheme
		scheme.tag = tag
		scheme
	}
}
