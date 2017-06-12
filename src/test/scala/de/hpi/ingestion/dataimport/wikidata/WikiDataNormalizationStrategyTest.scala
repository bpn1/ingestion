package de.hpi.ingestion.dataimport.wikidata

import org.scalatest.{FlatSpec, Matchers}

class WikiDataNormalizationStrategyTest extends FlatSpec with Matchers {
	"normalizeSector" should "extract and map sector" in {
		val sectors = TestData.unnormalizedSectors
		val result = WikiDataNormalizationStrategy.normalizeSector(sectors)
		val expected = TestData.normalizedSectors
		result shouldEqual expected
	}

	"normalizeCoords" should "extract lat;long from coordination String" in {
		val coordinates = TestData.unnormalizedCoords
		val result = WikiDataNormalizationStrategy.normalizeCoords(coordinates)
		val expected = TestData.normalizedCoords
		result shouldEqual expected
	}

	"normalizeCity" should "filter WikiData Ids" in {
		val cities = TestData.unnormalizedCities
		val result = WikiDataNormalizationStrategy.normalizeCity(cities)
		val expected = TestData.normalizedCities
		result shouldEqual expected
	}

	"normalizeCountry" should "normalize all possible appearances of country values" in {
		val countries = TestData.unnormalizedCountries
		val result = WikiDataNormalizationStrategy.normalizeCountry(countries)
		val expected = TestData.normalizedCountries
		result shouldEqual expected
	}

	"normalizeEmployees" should "normalize employees values" in {
		val employees = TestData.unnormalizedEmployees
		val result = WikiDataNormalizationStrategy.normalizeEmployees(employees)
		val expected = TestData.normalizedEmployees
		result shouldEqual expected
	}

	"normalizeURLs" should "validate urls" in {
		val urls = TestData.unnormalizedURLs
		val result = WikiDataNormalizationStrategy.normalizeURLs(urls)
		val expected = TestData.normalizedURLs
		result shouldEqual expected
	}

	"apply" should "return the right normalization method based on a given attribute" in {
		val inputs = TestData.applyInput
		val attributes = TestData.applyAttributes
		val results = attributes.map(WikiDataNormalizationStrategy.apply)
		val strategies = TestData.applyStrategies
		(results, strategies, inputs).zipped
			.map { case (result, expected, input) =>
				(result(input), expected(input))
			}
			.foreach { case (result, expected) =>
				result shouldEqual expected
			}
	}
}
