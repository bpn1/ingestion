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
  * Case class representing a link between Wikipedia pages.
  *
  * @param alias   text this link appears as on the page
  * @param page    page this link points to
  * @param offset  character offset of this alias in the plain text of the page it appears in
  * @param context term frequencies of the context of this link
  * @param article article from which the link was retrieved
  */
case class Link(
	alias: String,
	var page: String,
	var offset: Option[Int] = None,
	var context: Map[String, Int] = Map[String, Int](),
	var article: Option[String] = None
)
