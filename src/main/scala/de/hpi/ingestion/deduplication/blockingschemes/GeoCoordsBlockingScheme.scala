/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.deduplication.blockingschemes

import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.implicits.CollectionImplicits._

/**
  * This class uses geo coordinates as key.
  */
class GeoCoordsBlockingScheme extends BlockingScheme {
	tag = "GeoCoordsBlockingScheme"
	var decimalPower = 10
	override def generateKey(subject: Subject): List[String] = {
		val coords = subject.properties.get("geo_coords")
		coords.map {
			_.flatMap { coord =>
				val Array(lat, long) = coord.split(";").map(_.toDouble)
				val lat1 = math.floor(lat * decimalPower) / decimalPower
				val long1 = math.floor(long * decimalPower) / decimalPower
				val lat2 = math.floor(lat * decimalPower + 1) / decimalPower
				val long2 = math.floor(long * decimalPower + 1) / decimalPower
				val keys = List(lat1, lat2).cross(List(long1, long2))
				keys.map(_.productIterator.mkString(";"))
			}
		}.getOrElse(List(undefinedValue))
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
	def apply(tag: String, decimals: Double = 1.0): GeoCoordsBlockingScheme = {
		val scheme = new GeoCoordsBlockingScheme
		scheme.decimalPower = Math.pow(10.0, decimals).toInt
		scheme.tag = tag
		scheme
	}
}
