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
  * Case class representing a sentence in an article
  *
  * @param articletitle  title of the article the sentence was extracted from
  * @param articleoffset offset in the article the sentence was extracted from
  * @param text          text of the sentence
  * @param entities      found entity links in this article
  * @param bagofwords    bag of words of the sentence except for the aliases of the entities
  */
case class Sentence(
	articletitle: String,
	articleoffset: Int,
	text: String,
	entities: List[EntityLink],
	bagofwords: List[String]
)
