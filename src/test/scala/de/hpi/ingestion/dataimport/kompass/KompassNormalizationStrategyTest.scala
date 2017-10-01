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

package de.hpi.ingestion.dataimport.kompass

import org.scalatest.{FlatSpec, Matchers}

class KompassNormalizationStrategyTest extends FlatSpec with Matchers {
	"Legal form" should "be normalized" in {
		val legalForms = List(
			"Gesellschaft bürgerlichen Rechts (GbR)",
			"Offene Handelsgesellschaft (OHG)",
			"(Unternehmergesellschaft blabla Haftungsbeschränkt)",
			"Unternehmergesellschaft blabla mbH bla",
			"Unternehmergesellschaft blabla mbH bla & Co. KG",
			"Gesellschaft blabla mit beschränkter Haftung bla",
			"Unternehmergesellschaft blabla Haftungsbeschränkt & Co. KG",
			"Unternehmergesellschaft blabla mbH bla & Co. KG",
			"Gesellschaft blabla mit beschränkter Haftung bla & Co. KG",
			"GbR|GbR",
			"oHG|Offene Handelsgesellschaft"
		)
		val normalizedLegalForms = KompassNormalizationStrategy.normalizeLegalForm(legalForms)
		val expectedLegalForms = List("GbR", "OHG", "GmbH", "GmbH & Co. KG")
		normalizedLegalForms shouldEqual expectedLegalForms
	}

	"Turnover" should "be normalized into money values and ranges" in {
		val turnoverValues = List(
			"2 - 5 Millionen EUR",
			"< 500 000 EUR",
			"1 Million EUR",
			"7 Millionen EUR",
			"500 000 - 1 Million EUR",
			"5.000.000 EUR",
			"-",
			"0 EUR",
			"Unbekannt"
		)
		val normalizedTurnoverValues = KompassNormalizationStrategy.normalizeTurnover(turnoverValues)
		val expectedTurnoverValues = List("2000000", "5000000", "0", "500000", "1000000", "7000000")
		normalizedTurnoverValues shouldEqual expectedTurnoverValues
	}

	"Capital" should "be normalized" in {
		val capitalValues = List(
			"250.001 EUR",
			"1.854.400.000 EUR",
			"0 EUR",
			"250.001 EUR",
			"filterme"
		)
		val normalizedCapitalValues = KompassNormalizationStrategy.normalizeCapital(capitalValues)
		val expectedCapitalValues = List("250001", "1854400000", "0")
		normalizedCapitalValues shouldEqual expectedCapitalValues
	}

	"Employees" should "be normalized" in {
		val employeeNumbers = List(
			"Von 10 bis 19 Beschäftigte",
			"Von 0 bis 9 Beschäftigte",
			"keine Angabe",
			"Mehr als 5000 Beschäftigte",
			"30 Beschäftigte",
			"Von 10 bis 19 Beschäftigte"
		)
		val normalizedEmployeeNumbers = KompassNormalizationStrategy.normalizeEmployees(employeeNumbers)
		val expectedEmployeeNumbers = List("10", "19", "0", "9", "5000", "30")
		normalizedEmployeeNumbers shouldEqual expectedEmployeeNumbers
	}

	"Address fields" should "be extracted and normalized" in {
		val address = List("Dachsteinstraße 7 65199 Wiesbaden Deutschland")
		val badAddress = "Kaputte Adresse"
		val street = KompassNormalizationStrategy.normalizeStreet(address)
		street shouldEqual List("Dachsteinstraße 7")
		val postal = KompassNormalizationStrategy.normalizePostal(address)
		postal shouldEqual List("65199")
		val city = KompassNormalizationStrategy.normalizeCity(address)
		city shouldEqual List("Wiesbaden")
		val country = KompassNormalizationStrategy.normalizeCountry(address)
		country shouldEqual List("DE")
		val parsedBadAddress = KompassNormalizationStrategy.extractAddress(badAddress, "geo_street")
		parsedBadAddress shouldBe empty
	}

	"Attributes" should "be cleaned of duplicates and empty values" in {
		val attribute = List("1", "2", "3", "2", "")
		val normalizedAttribute = KompassNormalizationStrategy.normalizeDefault(attribute)
		val expectedAttribute = List("1", "2", "3")
		normalizedAttribute shouldEqual expectedAttribute
	}

}
