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
  * Case class representing an article preprocessed for NEL, e.g, a newspaper or Wikipedia article.
  *
  * @param id		     identifier of the article within its data source, e.g., a title or an ID
  * @param title	     title of the article
  * @param text          raw content of the article
  * @param triealiases   the longest aliases found by the trie with their offset and context
  * @param foundentities entities found by the classifier
  */
case class TrieAliasArticle(
	id: String,
	title: Option[String] = None,
	text: Option[String] = None,
	triealiases: List[TrieAlias] = Nil,
	foundentities: List[Link] = Nil
) {
	def getText: String = text.getOrElse("")
}
