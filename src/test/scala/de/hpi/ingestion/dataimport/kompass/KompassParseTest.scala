package de.hpi.ingestion.dataimport.kompass

import java.io.File
import java.util.UUID

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.dataimport.kompass.models.KompassEntity
import org.scalatest.{FlatSpec, Matchers}
import play.api.libs.json._
import de.hpi.ingestion.implicits.CollectionImplicits._

import scala.io.Source

class KompassParseTest extends FlatSpec with SharedSparkContext with Matchers {

// scalastyle:off number.of.methods
// scalastyle:off line.size.limit
	"fillEntityValues" should "Extract data from the input json and maps them to a Kompass entity." in {
		val file = """.\src\test\resources\dataimport\kompass\kompass_testData.json"""
		for (line <- Source.fromFile(file).getLines) {
			val json: JsValue = Json.parse(line)
			val kompassEntity = KompassParse
				.fillEntityValues(json)
				.copy(id = UUID.fromString("4071af38-78cb-4e1f-8aa7-ea47b29a1a6e"))
			val result = kompassEntity.copy(data = kompassEntity.data.filterKeys(key => (key != "employees") && (key != "Kapital")))
			val expected = KompassEntity(Some("Trans-Tech Weisser GmbH"),
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
					//"employees" -> List("Von 10 bis 19 Beschäftigte"),
					"sector" -> List("Transport & Logistik"),
					//"Kapital" -> List("250.001 EUR"),
					"Art des Unternehmens" -> List("Hauptsitz"),
					"county" -> List("Bayern"),
					"Rechtliche Hinweise" -> List("Gesellschaft mit beschränkter Haftung (GmbH)"),
					"address" -> List("Römerstraße 11 86438 Kissing Deutschland"),
					"district" -> List("Aichach-Friedberg"),
					"executives" -> List("Frau Carmen Hysek"),
					"turnover" -> List("2 - 5 Millionen EUR")
				),
				UUID.fromString("4071af38-78cb-4e1f-8aa7-ea47b29a1a6e")
			)
			println(result.data.printableSetDifference(expected.data))
			result shouldEqual expected
		}
	}
// scalastyle:on number.of.methods
// scalastyle:on line.size.limit

}
