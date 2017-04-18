package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.textmining.models.{Link, ParsedWikipediaEntry, TrieNode}
import org.scalatest.{FlatSpec, Matchers}

class TrieBuilderTest extends FlatSpec with Matchers with SharedSparkContext {

	"TrieBuilder" should "find matches" in {
		val resultEntry = testEntryFoundAliases()
		//resultEntry.foundaliases should not be empty
	}

	it should "not create duplicate entries" in {
		val resultEntry = testEntryFoundAliases()
		resultEntry.foundaliases.size shouldEqual resultEntry.foundaliases.distinct.size
	}

	it should "not find empty string matches" in {
		val resultEntry = testEntryFoundAliases()
		resultEntry.foundaliases should not contain ""
	}

	"Partitioned Trie" should "be properly initialized" in {
		val trie = TrieBuilder.initializeTrie(sc)
		//trie.fileName()
	}

	def testEntryFoundAliases(): ParsedWikipediaEntry = {
		val entry = TestData.parsedTestEntry()
		val trie = new TrieNode()
		val tokenizer = new WhitespaceTokenizer()
		for(link <- entry.allLinks) {
			trie.append(tokenizer.tokenize(link.alias))
		}
		//TrieBuilder.matchEntry(entry, trie, tokenizer)
		entry
	}
}
