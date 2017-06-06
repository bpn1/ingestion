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
