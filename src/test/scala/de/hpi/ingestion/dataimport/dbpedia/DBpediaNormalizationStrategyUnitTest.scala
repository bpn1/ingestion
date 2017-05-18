package de.hpi.ingestion.dataimport.dbpedia

import org.scalatest.{FlatSpec, Matchers}

class DBpediaNormalizationStrategyUnitTest extends FlatSpec with Matchers {
	"apply" should "decide, which strategy should be used regarding the input attribute" in {
		val inputs = List(
			TestData.unnormalizedEmployees,
			TestData.unnormalizedCoords,
			TestData.unnormalizedCountries,
			TestData.unnormalizedCities,
			TestData.unnormalizedSectors,
			List("default")
		)
		val attributes = List("gen_employees", "geo_coords", "geo_country", "geo_city", "gen_sectors", "default")
		val results = attributes.map(DBpediaNormalizationStrategy(_))
		val strategies: List[(List[String] => List[String])] = List(
			DBpediaNormalizationStrategy.normalizeEmployees,
			DBpediaNormalizationStrategy.normalizeCoords,
			DBpediaNormalizationStrategy.normalizeCountry,
			DBpediaNormalizationStrategy.normalizeCity,
			DBpediaNormalizationStrategy.normalizeSector,
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

	"normalizeCoords" should "kick out redundant information or integers" in {
		val coordinates = TestData.unnormalizedCoords
		val result = DBpediaNormalizationStrategy.normalizeCoords(coordinates)
		val expected = TestData.normalizedCoords
		result shouldEqual expected
	}

	"normalizeCountry" should "normalize all possible appearances of country values" in {
		val countries = TestData.unnormalizedCountries
		val result = DBpediaNormalizationStrategy.normalizeCountry(countries)
		val expected = TestData.normalizedCountries
		result shouldEqual expected
	}

	"normalizeEmployees" should "normalize the employees count" in {
		val employeesCount = TestData.unnormalizedEmployees
		val result = DBpediaNormalizationStrategy.normalizeEmployees(employeesCount)
		val expected = TestData.normalizedEmployees
		result shouldEqual expected
	}

	"normalizeSector" should "normalize and map the sectors" in {
		val sectors = TestData.unnormalizedSectors
		val result = DBpediaNormalizationStrategy.normalizeSector(sectors)
		val expected = TestData.normalizedSectors
		result shouldEqual expected
	}

	"normalizeCity" should "normalize all possible appearances of city values" in {
		val cities = TestData.unnormalizedCities
		val result = DBpediaNormalizationStrategy.normalizeCity(cities)
		val expected = TestData.normalizedCities
		result shouldEqual expected
	}
}
