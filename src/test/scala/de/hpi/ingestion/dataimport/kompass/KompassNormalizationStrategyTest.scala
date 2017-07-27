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
	"normalizeLegalForm" should "normalize most of the legal forms in kompass" in {
		val result = KompassNormalizationStrategy.normalizeLegalForm(List(
			"Gesellschaft bürgerlichen Rechts (GbR)",
			"Offene Handelsgesellschaft (OHG)",
			"(Unternehmergesellschaft blabla Haftungsbeschränkt)",
			"Unternehmergesellschaft blabla mbH bla",
			"Gesellschaft blabla mit beschränkter Haftung bla",
			"Unternehmergesellschaft blabla Haftungsbeschränkt & Co. KG",
			"Unternehmergesellschaft blabla mbH bla & Co. KG",
			"Gesellschaft blabla mit beschränkter Haftung bla & Co. KG",
			"GbR|GbR",
			"oHG|Offene Handelsgesellschaft"
		))
		val expected = List(
			"GbR",
			"OHG",
			"GmbH",
			"GmbH & Co. KG"
		)
		result shouldEqual expected
	}

	"normalizeTurnover" should "normalize money values and ranges" in {
		val result = KompassNormalizationStrategy.normalizeTurnover(List(
			"2 - 5 Millionen EUR",
			"< 500 000 EUR",
			"1 Million EUR",
			"7 Millionen EUR",
			"500 000 - 1 Million EUR",
			"5.000.000 EUR",
			"-",
			"0 EUR",
			"Unbekannt"
		))
		val expected = List(
			"2000000", "5000000",
			"0",
			"500000", "1000000",
			"7000000"
		)
		result shouldEqual expected
	}

	"normalizeCapital" should "normalize money values" in {
		val result = KompassNormalizationStrategy.normalizeCapital(List(
			"250.001 EUR",
			"1.854.400.000 EUR",
			"0 EUR",
			"250.001 EUR",
			"filterme"
		))
		val expected = List(
			"250001",
			"1854400000",
			"0"
		)
		result shouldEqual expected
	}

	"normalizeEmployees" should "normalize values of employee counts" in {
		val result = KompassNormalizationStrategy.normalizeEmployees(List(
			"Von 10 bis 19 Beschäftigte",
			"Von 0 bis 9 Beschäftigte",
			"keine Angabe",
			"Mehr als 5000 Beschäftigte",
			"30 Beschäftigte",
			"Von 10 bis 19 Beschäftigte"
		))
		val expected = List(
			"10", "19",
			"0", "9",
			"5000",
			"30"
		)
		result shouldEqual expected
	}

}
