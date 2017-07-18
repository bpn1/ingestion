package de.hpi.ingestion.dataimport.kompass

import java.util.UUID
import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.dataimport.kompass.models.KompassEntity
import org.scalatest.{FlatSpec, Matchers}
import play.api.libs.json._
import scala.io.Source

class KompassParseTest extends FlatSpec with SharedSparkContext with Matchers {

/*	"fillEntityValues" should "Extract data from the input json and map them to a Kompass entity." in {
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
		val expected = TestData.expectedKompassEntities
		result.toSet shouldEqual expected.toSet
	}
	*/
}
