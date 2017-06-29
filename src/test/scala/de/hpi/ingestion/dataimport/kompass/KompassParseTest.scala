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
	"fillEntityValues" should "Extract data from the input json and map them to a Kompass entity." in {
		val file = """.\src\test\resources\dataimport\kompass\kompass_testData.json"""
		var result = List[KompassEntity]()
		for (line <- Source.fromFile(file).getLines) {
			val jsonLine: JsValue = Json.parse(line)
			val kompassEntity = KompassParse
				.fillEntityValues(jsonLine)
				.copy(id = UUID.fromString("4071af38-78cb-4e1f-8aa7-ea47b29a1a6e"))
			result ++= List(kompassEntity
				.copy(data = kompassEntity.data.filterKeys(key => (key != "employees") && (key != "Kapital")))
			)
		}
		val expected = List(
			KompassEntity(
				Some("Trans-Tech Weisser GmbH"),
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
			),
			KompassEntity(
				Some("Möbel-Logistik GmbH Franken"),
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
		//println(result.head.data.printableSetDifference(expected.head.data))
		//println(result(1).data.printableSetDifference(expected(1).data))
		result.toSet shouldEqual expected.toSet


		/*for (line <- Source.fromFile(file).getLines) {
			val jsonLine: JsValue = Json.parse(line)
			val kompassEntity = KompassParse
				.fillEntityValues(jsonLine)
				.copy(id = UUID.fromString("4071af38-78cb-4e1f-8aa7-ea47b29a1a6e"))
			//val result = kompassEntity.copy(data = kompassEntity.data.filterKeys(key => (key != "employees") && (key != "Kapital")))
			val expected = List(KompassEntity(Some("Trans-Tech Weisser GmbH"),
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
			),
				KompassEntity(Some("Möbel-Logistik GmbH Franken"),
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
						"turnover" -> List("1 - 2 Millionen EUR"))
				),
				UUID.fromString("4071af38-78cb-4e1f-8aa7-ea47b29a1a6e")
			)*/
			//println(result.data.printableSetDifference(expected.data))

		//}
	}
// scalastyle:on number.of.methods
// scalastyle:on line.size.limit

}
