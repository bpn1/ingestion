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

import de.hpi.ingestion.implicits.RegexImplicits._
import de.hpi.ingestion.dataimport.SharedNormalizations

/**
  * Strategies for the normalization of Kompass entities
  */
object KompassNormalizationStrategy extends Serializable {

	/**
	  * Adds "& Co. KG" to a GmbH if fitting
	  * @param value extracted legal
	  * @return fully normalized legal
	  */
	def kgMatch(value: String): String = {
		val patternCoKg = "& Co\\. KG".r
		if(patternCoKg.findFirstIn(value).isDefined) "GmbH & Co. KG" else "GmbH"
	}

	/**
	  * Normalizes legal forms
	  * @param values legal form list
	  * @return normalized legals
	  */
	def normalizeLegalForm(values: List[String]): List[String] = {
		SharedNormalizations.normalizeLegalForm(values).map {
			case r""".*?\((.{0,4})${value}\)""" => value
			case r"""(?i).*?gesellschaft.*?haftungsbeschränkt(.*?)${value}""" => kgMatch(value)
			case r"""(?i).*gesellschaft.*mit beschränkter haftung(.*)${value}""" => kgMatch(value)
			case r""".*GbR.*""" => "GbR"
			case r"""(?i).*ohg.*""" => "OHG"
			case value => value
		}.distinct
	}

	/**
	  * Normalizes capital numbers
	  * @param values capital list
	  * @return normalized capitals
	  */
	def normalizeCapital(values: List[String]): List[String] = {
		values.flatMap {
			case r"""([0-9\.]+)${value} EUR""" => List(value)
			case _ => Nil
		}.map(_.replaceAll("( |\\.)", "")).distinct
	}

	/**
	  * Normalizes turnover numbers
	  * @param values turnover list
	  * @return normalized turnovers
	  */
	def normalizeTurnover(values: List[String]): List[String] = {
		values.flatMap {
			case r"""([0-9\.]+)${value} EUR""" => List(value)
			case r"""([0-9]+)${value} Million[en]{0,2} EUR""" => List(value + "000000")
			case r"""< ([0-9 ]+)${value} EUR""" => List("0", value)
			case r"""([0-9]+)${first} - ([0-9]+)${second} Millionen EUR""" => List(first, second).map(_ + "000000")
			case r"""([0-9 ]+)${first} - ([0-9]+)${second} Million[en]{0,2} EUR""" => List(first, second + "000000")
			case _ => Nil
		}.map(_.replaceAll("( |\\.)", "")).distinct
	}

	/**
	  * Normalizes employee numbers
	  * @param values employee list
	  * @return normalized employee numbers
	  */
	def normalizeEmployees(values: List[String]): List[String] = {
		values
			.flatMap {
				case r"""Von ([0-9]+)${first} bis ([0-9]+)${second} Beschäftigte""" => List(first, second)
				case r"""([0-9]+)${value} Beschäftigte""" => List(value)
				case r"""Mehr als ([0-9]+)${value} Beschäftigte""" => List(value)
				case _ => Nil
			}.map(_.replaceAll("( |\\.)", ""))
			.distinct
	}

	/**
	  * Extracts the address fields of the address string.
	  * @param address String containing the address
	  * @param property field of the address to extract
	  * @return normalized specified address field
	  */
	def extractAddress(address: String, property: String): Option[String] = {
		address match {
			case r"""(.+)${street} (\d{5})${postal} (.+)${city} Deutschland""" =>
				property match {
					case "geo_street" => Option(street.replaceFirst("str\\.", "straße"))
					case "geo_postal" => Option(postal)
					case "geo_city" => Option(city)
					case "geo_country" => Option("DE")
				}
			case _ => None
		}
	}

	/**
	  * Normalizes the street.
	  * @param values list containing the address data
	  * @return normalized street
	  */
	def normalizeStreet(values: List[String]): List[String] = {
		values.flatMap(extractAddress(_, "geo_street"))
	}

	/**
	  * Normalizes the postal code.
	  * @param values list containing the address data
	  * @return normalized postal code
	  */
	def normalizePostal(values: List[String]): List[String] = {
		values.flatMap(extractAddress(_, "geo_postal"))
	}

	/**
	  * Normalizes the city.
	  * @param values list containing the address data
	  * @return normalized city
	  */
	def normalizeCity(values: List[String]): List[String] = {
		values.flatMap {
			case address if address.endsWith("Deutschland") => extractAddress(address, "geo_city")
			case city => Option(city)
		}
	}

	/**
	  * Normalizes the country.
	  * @param values list containing the address data
	  * @return normalized country
	  */
	def normalizeCountry(values: List[String]): List[String] = {
		values.flatMap(extractAddress(_, "geo_country"))
	}

	/**
	  * The default normalization does not change the values.
	  * @param values Strings to be normalized
	  * @return normalized strings
	  */
	def normalizeDefault(values: List[String]): List[String] = {
		values
			.filter(_.nonEmpty)
			.distinct
	}

	/**
	  * Chooses the correct normalization method
	  * @param attribute Attribute to be normalized
	  * @return Normalization method
	  */
	def apply(attribute: String): (List[String]) => List[String] = {
		(attribute match {
			case "gen_legal_form" => normalizeLegalForm _
			case "gen_capital" => normalizeCapital _
			case "gen_turnover" => normalizeTurnover _
			case "gen_employees" => normalizeEmployees _
			case "geo_street" => normalizeStreet _
			case "geo_postal" => normalizePostal _
			case "geo_city" => normalizeCity _
			case "geo_country" => normalizeCountry _
			case _ => identity[List[String]] _
		}) compose normalizeDefault
	}

}
