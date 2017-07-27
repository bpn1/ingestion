/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.dataimport.dbpedia

import org.scalatest.{FlatSpec, Matchers}

class DBpediaNormalizationStrategyTest extends FlatSpec with Matchers {
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

	"normalizeURLs" should "normalize and validate urls" in {
		val urls = TestData.unnormalizedURLs
		val result = DBpediaNormalizationStrategy.normalizeURLs(urls)
		val expected = TestData.normalizedURLs
		result shouldEqual expected
	}

	"normalizeDefault" should "check for default patterns and normalize if possible or neccessary" in {
		val defaultValues = TestData.unnormalizedDefaults
		val result = DBpediaNormalizationStrategy.normalizeDefault(defaultValues)
		val expected = TestData.normalizedDefaults
		result shouldEqual expected
	}

	"apply" should "decide, which strategy should be used regarding the input attribute" in {
		val inputs = TestData.applyInput
		val attributes = TestData.applyAttributes
		val results = attributes.map(DBpediaNormalizationStrategy.apply)
		val strategies = TestData.applyStrategies
		(results, strategies, inputs).zipped
			.map { case (result, expected, input) =>
				(result(input), expected(input))
			}.foreach { case (result, expected) =>
				result shouldEqual expected
			}
	}
}
