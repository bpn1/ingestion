package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.textmining.models.{Link, ParsedWikipediaEntry, TrieNode}
import org.scalatest.{FlatSpec, Matchers}

class AliasTrieSearchTest extends FlatSpec with Matchers with SharedSparkContext {

	"Alias matches" should "be found" in {
		val tokenizer = IngestionTokenizer(new WhitespaceTokenizer, false, false)
		val trie = TestData.testDataTrie(tokenizer)
		val entry = TestData.parsedEntry()
		val resultEntry = AliasTrieSearch.matchEntry(entry, trie, tokenizer)
		resultEntry.foundaliases should not be empty
	}

	they should "be cleaned from duplicates and empty strings" in {
		val cleanedAliaes = AliasTrieSearch.cleanFoundAliases(TestData.uncleanedFoundAliaes())
		val expectedAliases = TestData.cleanedFoundAliaes()
		cleanedAliaes shouldEqual expectedAliases
	}

	"Trie" should "be properly deserialized" in {
		val trieStream = TestData.binaryTrieStream()
		val trie = AliasTrieSearch.deserializeTrie(trieStream)
		val expectedTrie = TestData.deserializedTrie()
		trie shouldEqual expectedTrie
	}
}
