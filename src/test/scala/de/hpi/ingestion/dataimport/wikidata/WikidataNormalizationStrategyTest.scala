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

package de.hpi.ingestion.dataimport.wikidata

import org.scalatest.{FlatSpec, Matchers}

class WikidataNormalizationStrategyTest extends FlatSpec with Matchers {
	"normalizeSector" should "extract and map sector" in {
		val sectors = TestData.unnormalizedSectors
		val result = WikidataNormalizationStrategy.normalizeSector(sectors)
		val expected = TestData.normalizedSectors
		result shouldEqual expected
	}

	"normalizeCoords" should "extract lat;long from coordination String" in {
		val coordinates = TestData.unnormalizedCoords
		val result = WikidataNormalizationStrategy.normalizeCoords(coordinates)
		val expected = TestData.normalizedCoords
		result shouldEqual expected
	}

	"normalizeCity" should "filter Wikidata Ids" in {
		val cities = TestData.unnormalizedCities
		val result = WikidataNormalizationStrategy.normalizeCity(cities)
		val expected = TestData.normalizedCities
		result shouldEqual expected
	}

	"normalizeCountry" should "normalize all possible appearances of country values" in {
		val countries = TestData.unnormalizedCountries
		val result = WikidataNormalizationStrategy.normalizeCountry(countries)
		val expected = TestData.normalizedCountries
		result shouldEqual expected
	}

	"normalizeEmployees" should "normalize employees values" in {
		val employees = TestData.unnormalizedEmployees
		val result = WikidataNormalizationStrategy.normalizeEmployees(employees)
		val expected = TestData.normalizedEmployees
		result shouldEqual expected
	}

	"normalizeURLs" should "validate urls" in {
		val urls = TestData.unnormalizedURLs
		val result = WikidataNormalizationStrategy.normalizeURLs(urls)
		val expected = TestData.normalizedURLs
		result shouldEqual expected
	}

	"apply" should "return the right normalization method based on a given attribute" in {
		val inputs = TestData.applyInput
		val attributes = TestData.applyAttributes
		val results = attributes.map(WikidataNormalizationStrategy.apply)
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
