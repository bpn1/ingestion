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

package de.hpi.ingestion.dataimport.kompass

import java.util.UUID
import de.hpi.ingestion.dataimport.kompass.models.KompassEntity

// scalastyle:off
object TestData {
	def expectedKompassEntities: List[KompassEntity] = {
		List(
			KompassEntity(
				Some("Trans-Tech Weisser GmbH"),
				Some("business"),
				Map(
					"specified_sector" -> List("Verpackungsanlagen, Verpackungsmaschinen und Verpackungsdienste"),
					"city" -> List("Kissing"),
					"url" -> List("http://de.kompass.com/c/trans-tech-weisser-gmbh/de657760/"),
					"activities" -> List(
						"Ausrichtmaschinen für Flaschen",
						"Verschliessmaschinen für Flaschen",
						"Förderanlagen für Flaschen",
						"Reinigungsmaschinen und Waschmaschinen für Flaschen",
						"Maschinen und Anlagen für das Abfüllen von Flaschen",
						"Dosenverschliessmaschinen",
						"Dosenförderanlagen und Förderanlagen für Dosendeckel",
						"Anlagen und Ausrüstungen für das Befüllen von Konserven",
						"Rollenförderer für Stückgüter",
						"Förderanlagen für Stückgut"
					),
					"Webseite" -> List("http://www.tthysek.de"),
					"Fax" -> List("+49 8233 791791"),
					"Gründungsjahr" -> List("1987"),
					"sector" -> List("Transport & Logistik"),
					"Art des Unternehmens" -> List("Hauptsitz"),
					"county" -> List("Bayern"),
					"Rechtliche Hinweise" -> List("Gesellschaft mit beschränkter Haftung (GmbH)"),
					"address" -> List("Römerstraße 11 86438 Kissing Deutschland"),
					"district" -> List("Aichach-Friedberg"),
					"executives" -> List("Frau Carmen Hysek"),
					"turnover" -> List("2 - 5 Millionen EUR")
				),
				UUID.fromString("4071af38-78cb-4e1f-8aa7-ea47b29a1a6e")
			),
			KompassEntity(
				Some("Möbel-Logistik GmbH Franken"),
				Some("business"),
				Map(
					"specified_sector" -> List("Transportwesen und Logistik"),
					"city" -> List("Itzgrund"),
					"url" -> List("http://de.kompass.com/c/mobel-logistik-gmbh-franken/de131073/"),
					"activities" -> List("Frachtinspektionsdienste und Frachtabnahmedienste"),
					"Fax" -> List("+49 9533 921152"),
					"Gründungsjahr" -> List("1996"),
					"sector" -> List("Transport & Logistik"),
					"Art des Unternehmens" -> List("Hauptsitz"),
					"county" -> List("Bayern"),
					"Rechtliche Hinweise" -> List("Gesellschaft mit beschränkter Haftung (GmbH)"),
					"address" -> List("Welsberg 36 96274 Itzgrund Deutschland"),
					"district" -> List("Landkreis Coburg"),
					"executives" -> List("Herr Günther Rose"),
					"turnover" -> List("1 - 2 Millionen EUR")
				),
				UUID.fromString("4071af38-78cb-4e1f-8aa7-ea47b29a1a6e")
			)
		)
	}

	def unnormalizedAddresses: List[String] = {
		List(
			"Graf-Adolf-Platz 15 40213 Düsseldorf Deutschland",
			"ThyssenKrupp Allee 1 45143 Essen Deutschland",
			"Altrottstr. 31 69190 Walldorf Deutschland",
			"Am TÜV 1 30519 Hannover Deutschland",
			"An der Universität 2 30823 Garbsen Deutschland",
			"Äußere Spitalhofstr. 19 94036 Passau Deutschland",
			"Nö!"
		)
	}

	def normalizedAddresses: List[Map[String, List[String]]] = {
		List(
			Map(
				"geo_street" -> List("Graf-Adolf-Platz 15"),
				"geo_postal" -> List("40213"),
				"geo_city" -> List("Düsseldorf"),
				"geo_country" -> List("DE")
			),
			Map(
				"geo_street" -> List("ThyssenKrupp Allee 1"),
				"geo_postal" -> List("45143"),
				"geo_city" -> List("Essen"),
				"geo_country" -> List("DE")
			),
			Map(
				"geo_street" -> List("Altrottstraße 31"),
				"geo_postal" -> List("69190"),
				"geo_city" -> List("Walldorf"),
				"geo_country" -> List("DE")
			),
			Map(
				"geo_street" -> List("Am TÜV 1"),
				"geo_postal" -> List("30519"),
				"geo_city" -> List("Hannover"),
				"geo_country" -> List("DE")
			),
			Map(
				"geo_street" -> List("An der Universität 2"),
				"geo_postal" -> List("30823"),
				"geo_city" -> List("Garbsen"),
				"geo_country" -> List("DE")
			),
			Map(
				"geo_street" -> List("Äußere Spitalhofstraße 19"),
				"geo_postal" -> List("94036"),
				"geo_city" -> List("Passau"),
				"geo_country" -> List("DE")
			),
			Map()
		)
	}
}

// scalastyle:on
