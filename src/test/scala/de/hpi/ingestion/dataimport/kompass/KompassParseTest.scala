package de.hpi.ingestion.dataimport.kompass

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import play.api.libs.json._

class KompassParseTest extends FlatSpec with SharedSparkContext with Matchers {

	"fillEntityValues" should "Extract data from the input json and maps them to a Kompass entity." in {
		val json: JsValue = Json.parse(
			"""{
				"name" : "Watership Down",
				"location" : {
					"lat" : 51.235685,
					"long" : -1.309197
				},
				"residents" : [ {
					"name" : "Fiver",
					"age" : 4,
					"role" : null
				}, {
					"name" : "Bigwig",
					"age" : 6,
					"role" : "Owsla"
				} ]
			}"""
		)
	}

}
