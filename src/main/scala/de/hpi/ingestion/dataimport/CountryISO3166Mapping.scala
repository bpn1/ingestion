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

package de.hpi.ingestion.dataimport

import java.util.Locale

/**
  * Map of countries to their ISO 3166 alpha-2 form
  * https://www.iso.org/glossary-for-iso-3166.html
  */
object CountryISO3166Mapping {
	val mapping: Map[String, String] = this.initMap

	def initMap: Map[String, String] = {
		val locales = Locale.getAvailableLocales
		val german = new Locale("de", "DE")
		locales
			.filter(_.getDisplayCountry(german).nonEmpty)
			.map(locale => locale.getDisplayCountry(german) -> locale.getCountry)
			.toMap
	}
}
