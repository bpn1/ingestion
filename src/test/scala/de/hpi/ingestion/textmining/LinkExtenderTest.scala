package de.hpi.ingestion.textmining

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.ParsedWikipediaEntry
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer


class LinkExtenderTest extends FlatSpec with Matchers with SharedSparkContext {

	"Pages" should "be empty" in {
		val pages = TestData.linkExtenderPagesMap()
		val entry = TestData.parsedEntry()
		val allPages = LinkExtender.findAllPages(entry, pages)
		allPages shouldBe empty
	}

	"Pages" should "exactly be these pages" in {
		val pages = TestData.linkExtenderPagesMap()
		val entry = TestData.linkExtenderParsedEntry()
		val allPages = LinkExtender.findAllPages(entry, pages)
		allPages shouldBe TestData.linkExtenderFoundPages()
	}


	"Aliases" should "be reverted from pages" in {
		val tokenizer = IngestionTokenizer(false, false)
		val pages = TestData.linkExtenderFoundPages()
		val aliases = LinkExtender.reversePages(pages, tokenizer)

		aliases shouldBe TestData.linkExtenderFoundAliases()
	}

	"Trie" should "include exactly these" in {
		val tokenizer = IngestionTokenizer(false, false)
		val aliases = TestData.linkExtenderFoundAliases()
		val trie = LinkExtender.buildTrieFromAliases(aliases, tokenizer)

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

	"Entry" should "have exactly these extended links" in {
		val entry = TestData.linkExtenderParsedEntry()
		val aliases = TestData.linkExtenderFoundAliases()
		val tokenizer = IngestionTokenizer(false, false)
		val trie = TestData.linkExtenderTrie(tokenizer)

		val parsedEntry = LinkExtender.findAliasOccurrences(
			entry,
			aliases,
			trie,
			tokenizer
		)

		parsedEntry.extendedlinks shouldEqual TestData.linkExtenderExtendedParsedEntry().head.extendedlinks
	}

	"Entry with extended Links" should "be exactly this entry" in {
		val entry = sc.parallelize(List(TestData.linkExtenderParsedEntry()))
		val pages = sc.parallelize(TestData.linkExtenderPagesSet().toList)
		val input = List(entry).toAnyRDD() ++ List(pages).toAnyRDD()
		val extendedEntries = LinkExtender.run(input, sc)
			.fromAnyRDD[ParsedWikipediaEntry]()
			.head
			.collect
			.toSet
		extendedEntries shouldEqual TestData.linkExtenderExtendedParsedEntry()
	}
	"Big Entry with extended Links" should "be exactly this big entry" in {
		val entry = sc.parallelize(List(TestData.bigLinkExtenderParsedEntry()))
		val pages = sc.parallelize(TestData.bigLinkExtenderPagesSet().toList)
		val input = List(entry).toAnyRDD() ++ List(pages).toAnyRDD()
		val extendedEntry = LinkExtender.run(input, sc)
			.fromAnyRDD[ParsedWikipediaEntry]()
			.head
			.collect
			.toSet
			.head
		extendedEntry.extendedlinks shouldEqual TestData.bigLinkExtenderExtendedParsedEntry().head.extendedlinks
	}
}
