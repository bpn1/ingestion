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
import de.hpi.ingestion.datalake.models.{Subject, Version}
import org.apache.spark.SparkContext

// scalastyle:off line.size.limit
// scalastyle:off method.length
object TestData {

	def version(sc: SparkContext): Version = Version("KompassDataLakeImport", List("dataSources"), sc, false, None)

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

	def kompassEntity: KompassEntity = {
		KompassEntity(
			name = Option("Kompass business"),
			instancetype = Option("business"),
			data = Map(
				"specified_sector" -> List("Transportmittel"),
				"city" -> List("Berlin"),
				"MwSt." -> List("DE123456789"),
				"url" -> List("http://de.kompass.com/c/kompass-business/"),
				"activities" -> List("Kraftfahrzeughandel / Kfz-Handel"),
				"Webseite" -> List("http://www.kompass-business.de"),
				"Fax" -> List("+49 123 1337666"),
				"Gründungsjahr" -> List("1337"),
				"employees" -> List("Von 13 bis 37 Beschäftigte"),
				"sector" -> List("Transport & Logistik"),
				"Art des Unternehmens" -> List("Hauptsitz"),
				"county" -> List("Berlin"),
				"Rechtliche Hinweise" -> List("Gesellschaft mit beschränkter Haftung"),
				"address" -> List("Leetstrase. 63 12345 Berlin Deutschland"),
				"district" -> List("Mitte"),
				"executives" -> List("Herr Max Mustermann"),
				"turnover" -> List("10 - 25 Millionen EUR")
			),
			id = UUID.fromString("e8b3749c-59e5-44b9-9597-3713888c451c")
		)
	}

	def translatedSubject: Subject = {
		Subject(
			master = UUID.randomUUID(),
			datasource = "kompass",
			category = Option("business"),
			properties = kompassEntity.data ++ Map(
				"geo_city" -> List("Berlin"),
				"id_tax" -> List("DE123456789"),
				"id_kompass" -> List("http://de.kompass.com/c/kompass-business/"),
				"gen_urls" -> List("http://www.kompass-business.de"),
				"gen_phones" -> List("+49 123 1337666"),
				"date_founding" -> List("1337"),
				"gen_employees" -> List("13", "37"),
				"gen_sectors" -> List("Transportmittel", "Transport & Logistik"),
				"geo_county" -> List("Berlin"),
				"gen_legal_form" -> List("GmbH"),
				"geo_street" -> List("Leetstrase. 63"),
				"geo_postal" -> List("12345"),
				"geo_country" -> List("DE"),
				"geo_county" -> List("Berlin", "Mitte"),
				"gen_ceo" -> List("Herr Max Mustermann"),
				"gen_turnover" -> List("10000000","25000000")
			)
		)
	}
}
// scalastyle:on method.length
// scalastyle:on line.size.limit
