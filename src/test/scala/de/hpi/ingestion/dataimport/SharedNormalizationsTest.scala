package de.hpi.ingestion.dataimport

import org.scalatest.{FlatSpec, Matchers}

class SharedNormalizationsTest extends FlatSpec with Matchers {
	"isValidUrl" should "validate URLs" in {
		val validURLs = TestData.validURLs.map(SharedNormalizations.isValidUrl)
		val invalidURLs = TestData.invalidURLs.map(SharedNormalizations.isValidUrl)

		validURLs.foreach(_ shouldBe true)
		invalidURLs.foreach(_ shouldBe false)
	}
}
