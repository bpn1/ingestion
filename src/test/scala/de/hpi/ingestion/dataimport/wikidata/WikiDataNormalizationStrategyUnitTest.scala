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

	"apply" should "return the right normalization method based on a given attribute" in {
		val inputs = List(
			TestData.unnormalizedCoordinates,
			TestData.unnormalizedCountries,
			List("default")
		)
		val attributes = List("geo_coords", "geo_country", "default")
		val results = attributes.map(WikiDataNormalizeStrategy(_))
		val strategies: List[(List[String] => List[String])] = List(
			WikiDataNormalizeStrategy.normalizeCoords, WikiDataNormalizeStrategy.normalizeCountry, identity
		)
		(results, strategies, inputs).zipped
			.map { case (result, expected, input) =>
				(result(input), expected(input))
			}
			.foreach { case (result, expected) =>
				result shouldEqual expected
			}

	}
}
