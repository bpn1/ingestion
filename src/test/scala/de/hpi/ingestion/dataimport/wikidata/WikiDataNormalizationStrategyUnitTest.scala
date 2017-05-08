package de.hpi.ingestion.dataimport.wikidata

import org.scalatest.{FlatSpec, Matchers}

class WikiDataNormalizationStrategyUnitTest extends FlatSpec with Matchers {
	"normalizeCoords" should "extract lat;long from coordination String" in {
		val coordinates = TestData.unnormalizedCoordinates
		val result = WikiDataNormalizeStrategy.normalizeCoords(coordinates)
		val expected = TestData.normalizedCoordinates

		result shouldEqual expected
	}

	"normalizedCountries" should "filter WikiData Ids" in {
		val countries = TestData.unnormalizedCountries
		val result = WikiDataNormalizeStrategy.normalizeCountry(countries)
		val expected = TestData.normalizedCountries

		result shouldEqual expected
	}
}
