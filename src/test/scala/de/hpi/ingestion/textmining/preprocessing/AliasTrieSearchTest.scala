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

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.textmining.TestData
import de.hpi.ingestion.textmining.models.ParsedWikipediaEntry
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.scalatest.{FlatSpec, Matchers}

class AliasTrieSearchTest extends FlatSpec with Matchers with SharedSparkContext {
	"Alias matches" should "be found" in {
		val tokenizer = IngestionTokenizer()
		val trie = TestData.dataTrie(tokenizer)
		val entry = TestData.parsedEntry()
		val resultEntry = AliasTrieSearch.matchEntry(entry, trie, tokenizer)
		resultEntry.foundaliases should not be empty
	}

	they should "be cleaned of empty strings" in {
		val cleanedAliaes = AliasTrieSearch.cleanFoundAliases(TestData.uncleanedFoundAliases())
		val expectedAliases = TestData.cleanedFoundAliases()
		cleanedAliaes shouldEqual expectedAliases
	}

	"Trie" should "be properly deserialized" in {
		val trieStream = TestData.binaryTrieStream()
		val trie = AliasTrieSearch.deserializeTrie(trieStream)
		val expectedTrie = TestData.deserializedTrie()
		trie shouldEqual expectedTrie
	}

	"Wikipedia entries" should "be enriched with found aliases" in {
		val job = new AliasTrieSearch
		val testTrieStreamFunction = TestData.fullTrieStream() _
		job.trieStreamFunction = testTrieStreamFunction
		job.parsedWikipedia = sc.parallelize(Seq(TestData.parsedEntry()))
		job.run(sc)
		val enrichedEntry = job.parsedWikipediaWithAliases.collect.head
		val expectedAliases = TestData.parsedEntryFoundAliases()
		enrichedEntry.foundaliases.toSet shouldEqual expectedAliases
	}

	"Trie aliases" should "be extracted" in {
		val tokenizer = IngestionTokenizer()
		val trie = TestData.dataTrie(tokenizer)
		val entries = TestData.parsedEntriesWithLessText()
		val trieAliases = entries.map(AliasTrieSearch.matchEntry(_, trie, tokenizer).triealiases)
		val expected = TestData.foundTrieAliases()
		trieAliases shouldEqual expected
	}

	they should "not be overlapped by other trie aliases or links" in {
		val tokenizer = IngestionTokenizer()
		val trie = TestData.dataTrie(tokenizer, "/spiegel/triealiases")
		val entries = TestData.articlesWithoutTrieAliases()
		val trieAliases = entries.map(AliasTrieSearch.matchEntry(_, trie, tokenizer))
		val expected = TestData.articlesWithTrieAliases()
		trieAliases shouldEqual expected
	}
}
