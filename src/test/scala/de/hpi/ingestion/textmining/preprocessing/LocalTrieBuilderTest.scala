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

package de.hpi.ingestion.textmining.preprocessing

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import de.hpi.ingestion.textmining.TestData
import org.scalatest.{FlatSpec, Matchers}

class LocalTrieBuilderTest extends FlatSpec with Matchers {
	"Trie" should "be serialized" in {
		val aliasStream = TestData.aliasFileStream()
		val trieStream = new ByteArrayOutputStream(1024)
		LocalTrieBuilder.serializeTrie(aliasStream, trieStream)
		trieStream.size() should be > 0
	}

	"Serialized trie" should "contain the aliases" in {
		val aliasStream = TestData.aliasFileStream()
		val trieStream = new ByteArrayOutputStream(1024)
		LocalTrieBuilder.serializeTrie(aliasStream, trieStream)
		val trieInputStream = new ByteArrayInputStream(trieStream.toByteArray)
		val trie = AliasTrieSearch.deserializeTrie(trieInputStream)
		val expectedTrie = TestData.deserializedTrie()
		trie shouldEqual expectedTrie
	}
}
