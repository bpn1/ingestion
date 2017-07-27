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

package de.hpi.ingestion.textmining.models

/**
  * An Alias appearing in Wikipedia containing the data about the pages it points to and how often it appears.
  *
  * @param alias            the alias as it appears in the readable text
  * @param pages            all Wikipedia pages this alias points to and how often it does
  * @param pagesreduced     all Wikipedia pages this alias points to and how often it does
  * @param linkoccurrences  in how many articles this alias appears as link
  * @param totaloccurrences in how many articles this alias appears in the plain text
  */
case class Alias(
	alias: String,
	pages: Map[String, Int] = Map(),
	pagesreduced: Map[String, Int] = Map(),
	var linkoccurrences: Option[Int] = None,
	var totaloccurrences: Option[Int] = None
)
