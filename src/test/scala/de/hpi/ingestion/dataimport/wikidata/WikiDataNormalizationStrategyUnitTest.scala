package de.hpi.ingestion.dataimport.wikidata

import org.scalatest.{FlatSpec, Matchers}

class WikiDataNormalizationStrategyUnitTest extends FlatSpec with Matchers {
	"normalizeSector" should "extract and map sector" in {
		val sectors = TestData.unnormalizedSectors
		val result = WikiDataNormalizationStrategy.normalizeSector(sectors)
		val expected = TestData.normalizedSectors
		result shouldEqual expected
	}

	"normalizeCoords" should "extract lat;long from coordination String" in {
		val coordinates = TestData.unnormalizedCoordinates
		val result = WikiDataNormalizationStrategy.normalizeCoords(coordinates)
		val expected = TestData.normalizedCoordinates
		result shouldEqual expected
	}

	"normalizeLocation" should "filter WikiData Ids" in {
		val locations = TestData.unnormalizedLocations
		val result = WikiDataNormalizationStrategy.normalizeLocation(locations)
		val expected = TestData.normalizedLocations
		result shouldEqual expected
	}

	"normalizeEmployees" should "normalize employees values" in {
		val employees = TestData.unnormalizedEmployees
		val result = WikiDataNormalizationStrategy.normalizeEmployees(employees)
		val expected = TestData.normalizedEmployees
		result shouldEqual expected
	}

	"apply" should "return the right normalization method based on a given attribute" in {
		val inputs = List(
			TestData.unnormalizedCoordinates,
			TestData.unnormalizedLocations,
			TestData.unnormalizedSectors,
			TestData.unnormalizedEmployees,
			List("default")
		)
		val attributes = List("geo_coords", "geo_country", "gen_sectors", "gen_employees", "default")
		val results = attributes.map(WikiDataNormalizationStrategy(_))
		val strategies: List[(List[String] => List[String])] = List(
			WikiDataNormalizationStrategy.normalizeCoords,
			WikiDataNormalizationStrategy.normalizeLocation,
			WikiDataNormalizationStrategy.normalizeSector,
			WikiDataNormalizationStrategy.normalizeEmployees,
			identity
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
