package de.hpi.ingestion.dataimport.kompass

import java.io.File

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import play.api.libs.json._

import scala.io.Source

class KompassParseTest extends FlatSpec with SharedSparkContext with Matchers {

	"fillEntityValues" should "Extract data from the input json and maps them to a Kompass entity." in {
		val file = """.\src\test\resources\dataimport\kompass\kompass_testData.json"""
		for (line <- Source.fromFile(file).getLines) {
			val json: JsValue = Json.parse(line)
		}
	}

}
