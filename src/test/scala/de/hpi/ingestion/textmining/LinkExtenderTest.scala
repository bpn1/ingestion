package de.hpi.ingestion.textmining

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.{SharedSparkContext, RDDComparisons}
import de.hpi.ingestion.implicits.CollectionImplicits._

class LinkExtenderTest extends FlatSpec with Matchers with SharedSparkContext {

	"pages" should "be empty" in {
		val pages = TestData.linkExtenderPagesTestMap()
		val entry = TestData.parsedEntry()
		val allPages = LinkExtender.findAllAliases(entry, pages)
		allPages shouldBe empty
	}

	"pages" should "not be empty" in {
		val pages = TestData.linkExtenderPagesTestMap()
		val entry = TestData.linkExtenderParsedEntry()
		val allPages = LinkExtender.findAllAliases(entry, pages)
		allPages shouldBe TestData.linkExtenderFoundPages()
	}


	"aliases" should "be reverted from pages" in {
		val pages = TestData.linkExtenderFoundPages()
		val aliases = LinkExtender.reversePages(pages)

		aliases shouldBe TestData.linkExtenderFoundAliases()
	}

	"trie" should "include string" in {
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

	"entry" should "have extended Links" in {
		val entry = TestData.linkExtenderParsedEntry()
		val aliases = TestData.linkExtenderFoundAliases()
		val tokenizer = IngestionTokenizer(false, false)
		val trie = LinkExtender.buildTrieFromAliases(aliases, tokenizer)

		val parsedEntry = LinkExtender.findAliasOccurences(
			entry,
			aliases,
			trie,
			tokenizer
		)

		parsedEntry.extendedLinks should not be empty
	}

	"enrty with extended Links" should "be exactly this entry" in {
		val entry = sc.parallelize(List(TestData.linkExtenderParsedEntry()))
		val pages = sc.parallelize(TestData.linkExtenderPagesTestSet().toList)
		val input = List(entry).toAnyRDD() ++ List(pages).toAnyRDD()
		val extendedEntries = LinkExtender.run(input, sc)
		extendedEntries shouldBe empty
	}
}
