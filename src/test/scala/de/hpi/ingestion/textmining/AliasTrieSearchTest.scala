package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.ParsedWikipediaEntry
import de.hpi.ingestion.textmining.tokenizer.{IngestionTokenizer, WhitespaceTokenizer}
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

class AliasTrieSearchTest extends FlatSpec with Matchers with SharedSparkContext {

	"Alias matches" should "be found" in {
		val tokenizer = IngestionTokenizer(new WhitespaceTokenizer, false, false)
		val trie = TestData.testDataTrie(tokenizer)
		val entry = TestData.parsedEntry()
		val resultEntry = AliasTrieSearch.matchEntry(entry, trie, tokenizer)
		resultEntry.foundaliases should not be empty
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
		val testTrieStreamFunction = TestData.fullTrieStream _
		AliasTrieSearch.trieStreamFunction = testTrieStreamFunction
		val inputEntry = sc.parallelize(Seq(TestData.parsedEntry()))
		val searchResult = AliasTrieSearch.run(List(inputEntry).toAnyRDD(), sc)
		val enrichedEntry = searchResult.fromAnyRDD[ParsedWikipediaEntry]().head.collect.head
		val foundAliases = TestData.parsedEntryFoundAliases()
		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
		enrichedEntry.foundaliases.toSet shouldEqual foundAliases
	}
}
