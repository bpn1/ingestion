package de.hpi.ingestion.dataimport

import org.scalatest.{FlatSpec, Matchers}

class SharedNormalizationsTest extends FlatSpec with Matchers {
	"isValidUrl" should "validate URLs" in {
		val validURLs = TestData.validURLs.map(SharedNormalizations.isValidUrl)
		val invalidURLs = TestData.invalidURLs.map(SharedNormalizations.isValidUrl)

		validURLs.forall(identity) shouldBe true
		invalidURLs.exists(identity) shouldBe false
	}

	"normalizeLegalForm" should "normalize a legal forms to its abbreviation" in {
		val legalForms = TestData.unnormalizedLegalForms
		val normalized = SharedNormalizations.normalizeLegalForm(legalForms)
		val expected = TestData.normalizedLegalForms

		normalized shouldEqual expected
	}
}
