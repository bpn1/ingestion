package de.hpi.ingestion.dataimport.dbpedia

import org.scalatest.{FlatSpec, Matchers}

class DBpediaNormalizeStrategyUnitTest extends FlatSpec with Matchers {
	"apply" should "decide, which strategy should be used regarding the input attribute" in {
		val inputs = List(
			TestData.unnormalizedCoords,
			TestData.unnormalizedCountries,
			List("default")
		)
		val attributes = List("gen_employees", "geo_country", "geo_coords", "default")
		val results = attributes.map(DBpediaNormalizeStrategy(_))
		val strategies: List[(List[String] => List[String])] = List(
			DBpediaNormalizeStrategy.normalizeEmployees,
			DBpediaNormalizeStrategy.normalizeCountry,
			DBpediaNormalizeStrategy.normalizeCoords,
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
		val result = DBpediaNormalizeStrategy.normalizeCoords(coordinates)
		val expected = TestData.normalizedCoords
		result shouldEqual expected
	}

	"normalizeCountry" should "normalize all possible appearances of country values" in {
		val countries = TestData.unnormalizedCountries
		val result = DBpediaNormalizeStrategy.normalizeCountry(countries)
		val expected = TestData.normalizedCountries
		result shouldEqual expected
	}

	"normalizeEmployees" should "normalize the employees count" in {
		val employeesCount = TestData.unnormalizedEmployees
		val result = DBpediaNormalizeStrategy.normalizeEmployees(employeesCount)
		val expected = TestData.normalizedEmployees
		result shouldEqual expected
	}
}
