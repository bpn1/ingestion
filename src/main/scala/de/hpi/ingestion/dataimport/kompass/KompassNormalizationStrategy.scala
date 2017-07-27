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
	def kgMatch(value: String): List[String] = {
		val patternCoKg = "& Co\\. KG".r
		if (patternCoKg.findFirstIn(value).isDefined) List("GmbH & Co. KG") else List("GmbH")
	}

	/**
	  * Normalizes legal forms
	  * @param values legal form list
	  * @return normalized legals
	  */
	def normalizeLegalForm(values: List[String]): List[String] = {
		SharedNormalizations.normalizeLegalForm(values).flatMap {
			case r""".*?\((.{0,4})${value}\)""" => List(value)
			case r"""(?i).*?gesellschaft.*?haftungsbeschränkt(.*?)${value}""" => kgMatch(value)
			case r"""(?i).*gesellschaft.*mbh(.*)${value}""" => kgMatch(value)
			case r"""(?i).*gesellschaft.*mit beschränkter haftung(.*)${value}""" => kgMatch(value)
			case r""".*GbR.*""" => List("GbR")
			case r"""(?i).*ohg.*""" => List("OHG")
			case value => List(value)
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
		values.flatMap {
			case r"""Von ([0-9]+)${first} bis ([0-9]+)${second} Beschäftigte""" => List(first, second)
			case r"""([0-9]+)${value} Beschäftigte""" => List(value)
			case r"""Mehr als ([0-9]+)${value} Beschäftigte""" => List(value)
			case _ => Nil
		}.map(_.replaceAll("( |\\.)", "")).distinct
	}

	/**
	  * Chooses the correct normalization method
	  * @param attribute Attribute to be normalized
	  * @return Normalization method
	  */
	def apply(attribute: String): (List[String]) => List[String] = {
		attribute match {
			case "gen_legal_form" => normalizeLegalForm
			case "gen_capital" => normalizeCapital
			case "gen_turnover" => normalizeTurnover
			case "gen_employees" => normalizeEmployees
			case _ => identity
		}
	}

}
