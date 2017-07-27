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

import de.hpi.ingestion.implicits.RegexImplicits._
import de.hpi.ingestion.dataimport.{CountryISO3166Mapping, SharedNormalizations}
import de.hpi.ingestion.dataimport.SharedNormalizations

/**
  * Strategies for the normalization of DBpedia entities
  */
object DBpediaNormalizationStrategy extends Serializable {
	/**
	  * Normalizes Employees
	  * @param values employees list
	  * @return normalizes employees
	  */
	def normalizeEmployees(values: List[String]): List[String] = {
		values.flatMap {
			case r"""([-+]?[0-9]+\.?[0-9]*)${employees}\^\^xsd:integer""" => List(employees)
			case r"""([-+]?[0-9]+\.?[0-9]*)${employees}\^\^xsd:nonNegativeInteger""" => List(employees)
			case r"""über (\d+)${employees}@de \.""" => List(employees)
			case _ => None
		}.map(_.replaceAll("(_|-)", " ")).distinct
	}

	/**
	  * Normalizes countries
	  * @param values country list
	  * @return normalized countries
	  */
	def normalizeCountry(values: List[String]): List[String] = {
		values.flatMap {
			case r"""dbpedia-de:([A-Za-zÄäÖöÜüß\-_]{3,})${country}""" => CountryISO3166Mapping.mapping.get(country)
			case r"""dbpedia-de:([A-Za-zÄäÖöÜüß\-_]{2})${country}""" => List(country)
			case r"""([A-Za-zÄäÖöÜüß\-]{3,})${country}@de \.""" => CountryISO3166Mapping.mapping.get(country)
			case r"""([A-Za-zÄäÖöÜüß\-]{2})${country}@de \.""" => List(country)
			case _ => None
		}.map(_.replaceAll("(_|-)", " ")).distinct
	}

	/**
	  * Normalizes coordinates
	  * @param values coordinates list
	  * @return normalized coordinates list
	  */
	def normalizeCoords(values: List[String]): List[String] = {
		values.flatMap {
			case r"""(\d+\.\d*)${lat}\^\^xsd:.+;(\d+\.\d*)${long}\^\^xsd:.+""" => List(s"$lat;$long")
			case _ => None
		}.distinct
	}

	/**
	  * Normalizes sector
	  * @param values sector list
	  * @return normalized sectors
	  */
	def normalizeSector(values: List[String]): List[String] = {
		values.flatMap {
			case r"""dbpedia-de:([A-Za-zÄäÖöÜüß\-_]+)${sector}""" => List(sector)
			case r"""([A-Za-zÄäÖöÜüß\-_]+)${sector}@de \.""" => List(sector)
			case _ => None
		}.distinct
	}

	/**
	  * Normalizes cities
	  * @param values city list
	  * @return normalized cities
	  */
	def normalizeCity(values: List[String]): List[String] = {
		values.flatMap {
			case r"""dbpedia-de:(.+)${city}""" => List(city)
			case r"""(.+)${city}@de \.""" => List(city)
			case other => List(other)
		}.map(_.replaceAll("(_|-)", " ")).distinct
	}

	/**
	  * Normalize urls
	  * @param values list of urls
	  * @return valid urls
	  */
	def normalizeURLs(values: List[String]): List[String] = {
		values.flatMap {
			case r"""(.+)${value}@de \.""" => List(value)
			case other => List(other)
		}.filter(SharedNormalizations.isValidUrl)
	}

	/**
	  * Normalize legal form
	  * @param values list of legal forms
	  * @return normalized legal form
	  */
	def normalizeLegalForm(values: List[String]): List[String] = {
		val legalForm = values.flatMap {
			case r"""dbpedia-de:(.+)${legalForm}""" => List(legalForm)
			case r"""(.+)${legalForm}@de \.""" => List(legalForm)
			case legalForm => List(legalForm)
		}.map(_.replaceAll("(_|-)", " ")).distinct
		SharedNormalizations.normalizeLegalForm(legalForm)
	}

	/**
	  * Normalizes all other values by default (removing pre- and suffixes and dashes)
	  * @param values Strings to be normalized
	  * @return normalized strings
	  */
	def normalizeDefault(values: List[String]): List[String] = {
		values.flatMap {
			case r"""(.+)${value}\^\^xsd:.+""" => List(value)
			case r"""(.+)${value}@de \.""" => List(value)
			case r"""dbpedia-de:(.+)${value}""" => List(value)
			case other => List(other)
		}.map(_.replaceAll("(_|-)", " ")).distinct
	}

	/**
	  * Chooses the right normalization method
	  * @param attribute Attribute to be normalized
	  * @return Normalization method
	  */
	def apply(attribute: String): (List[String]) => List[String] = {
		attribute match {
			case "gen_sectors" => this.normalizeSector
			case "gen_employees" => this.normalizeEmployees
			case "geo_country" => this.normalizeCountry
			case "geo_coords" => this.normalizeCoords
			case "geo_city" => this.normalizeCity
			case "gen_urls" => this.normalizeURLs
			case "gen_legal_form" => this.normalizeLegalForm
			case _ => this.normalizeDefault
		}
	}
}
