package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class LinkExtenderTest extends FlatSpec with Matchers with SharedSparkContext {

	"aliases" should "be empty" in {
		val aliases = TestData.linkExtenderPagesTestSet()
		val entry = TestData.parsedEntry()
		val allAliases = LinkExtender.findAllAliases(entry, sc.parallelize(aliases.toList))
		allAliases shouldBe empty
	}

	"aliases" should "not be empty" in {
		val aliases = TestData.linkExtenderPagesTestSet()
		val entry = TestData.linkExtenderParsedEntry()
		val allAliases = LinkExtender.findAllAliases(entry, sc.parallelize(aliases.toList))
		allAliases shouldBe TestData.linkExtenderFoundPages()
	}

	"trie" should "include string" in {
		val aliases = TestData.linkExtenderFoundPages()
		val trie = LinkExtender.buildTrieFromAliases(aliases)

		val result = trie.findByPrefix(List[String]("Audi"))

		result should have length 2
		result should contain (List[String]("Audi"))
		result should contain (List[String]("Audi", "AG"))

		val result2 = trie.findByPrefix(List[String]("VW"))

		result2 should have length 1
		result2 should contain (List[String]("VW"))

		val result3 = trie.findByPrefix(List[String]("Volkswagen"))

		result3 should have length 1
		result3 should contain (List[String]("Volkswagen", "AG"))
	}

	"entry" should "have extended Links" in {
		val entry = TestData.linkExtenderParsedEntry()
		val pages = TestData.linkExtenderFoundPages()
		val trie = LinkExtender.buildTrieFromAliases(pages)
		val tokenizer = IngestionTokenizer(false, false)

		val parsedEntry = LinkExtender.findAliasOccurences(
			entry,
			pages,
			trie,
			tokenizer
		)

		parsedEntry.extendedLinks should not be empty
	}
}
