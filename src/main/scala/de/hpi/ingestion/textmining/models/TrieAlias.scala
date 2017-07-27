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
  * Represents an alias found with the trie and contains the alias, its index and its context.
  *
  * @param alias   alias found in the text
  * @param offset  character offset of the alias in the text
  * @param context term frequencies of the aliases context
  */
case class TrieAlias(
	alias: String,
	offset: Option[Int] = None,
	context: Map[String, Int] = Map()
) {
	def toLink(): Link = {
		Link(alias, null, offset, context)
	}
}
