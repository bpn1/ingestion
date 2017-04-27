package de.hpi.ingestion.dataenrichment

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.{FlatSpec, Matchers}

class DataEnrichmentUnitTest extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {

	"getGeoCode" should "resolve a given postal address string into a geocode" in {

		val dataenrichment = new MapboxDataEnrichment
		val testAddress = "Straße 5 9, 13125 Berlin"

		val computedValue = dataenrichment.getGeoCode(testAddress)

		println(computedValue)
		val expectedValue = 200

		computedValue shouldEqual expectedValue
	}
}