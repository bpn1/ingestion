package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.ParsedWikipediaEntry
import de.hpi.ingestion.textmining.tokenizer.{IngestionTokenizer, WhitespaceTokenizer}
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

class AliasTrieSearchTest extends FlatSpec with Matchers with SharedSparkContext {

	"Alias matches" should "be found" in {
		val oldSettings = AliasTrieSearch.settings(false)
		AliasTrieSearch.parseConfig()

		val tokenizer = IngestionTokenizer()
		val contextTokenizer = IngestionTokenizer(true, true)
		val trie = TestData.dataTrie(tokenizer)
		val entry = TestData.parsedEntry()
		val resultEntry = AliasTrieSearch.matchEntry(entry, trie, tokenizer, contextTokenizer)
		resultEntry.foundaliases should not be empty

		AliasTrieSearch.settings = oldSettings
	}

	they should "be cleaned from duplicates and empty strings" in {
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
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldSettings = AliasTrieSearch.settings(false)
		AliasTrieSearch.parseConfig()

		val testTrieStreamFunction = TestData.fullTrieStream _
		AliasTrieSearch.trieStreamFunction = testTrieStreamFunction
		val inputEntry = sc.parallelize(Seq(TestData.parsedEntry()))
		val searchResult = AliasTrieSearch.run(List(inputEntry).toAnyRDD(), sc)
		val enrichedEntry = searchResult.fromAnyRDD[ParsedWikipediaEntry]().head.collect.head
		val expectedAliases = TestData.parsedEntryFoundAliases()
		enrichedEntry.foundaliases.toSet shouldEqual expectedAliases

		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
		AliasTrieSearch.settings = oldSettings
	}

	"Trie aliases" should "be extracted" in {
		val oldSettings = AliasTrieSearch.settings(false)
		AliasTrieSearch.parseConfig()

		val tokenizer = IngestionTokenizer()
		val contextTokenizer = IngestionTokenizer(true, true)
		val trie = TestData.dataTrie(tokenizer)
		val entries = TestData.parsedEntriesWithLessText()
		val trieAliases = entries.map(AliasTrieSearch.matchEntry(_, trie, tokenizer, contextTokenizer).triealiases)
		val expected = TestData.foundTrieAliases()
		trieAliases shouldEqual expected

		AliasTrieSearch.settings = oldSettings
	}

	"Alias contexts" should "be extracted" in {
		val oldSettings = AliasTrieSearch.settings(false)
		AliasTrieSearch.parseConfig()

		val tokenizer = IngestionTokenizer()
		val contextTokenizer = IngestionTokenizer(true, true)
		val text = tokenizer.processWithOffsets(TestData.parsedEntriesWithLessText().head.getText())
		val trieAlias = TestData.foundTrieAliases().head.head
		val alias = tokenizer.processWithOffsets(trieAlias.alias)
		val context = AliasTrieSearch.extractAliasContext(alias, text, 0, contextTokenizer).getCounts()
		context shouldEqual trieAlias.context

		AliasTrieSearch.settings = oldSettings
	}
}
