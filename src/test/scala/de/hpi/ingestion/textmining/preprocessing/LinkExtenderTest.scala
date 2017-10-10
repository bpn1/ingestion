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
		allPages shouldEqual TestData.linkExtenderFoundPages()
	}


	"Aliases" should "be reverted from pages" in {
		val tokenizer = IngestionTokenizer(false, false)
		val pages = TestData.linkExtenderFoundPages()
		val aliases = LinkExtender.reversePages(pages, tokenizer)
		aliases shouldEqual TestData.linkExtenderFoundAliases()
	}

	"Trie" should "include exactly these tokens" in {
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
		val expectedLinks = TestData.linkExtenderExtendedParsedEntry().head.extendedlinks(noTextLinks = false)
		parsedEntry.extendedlinks(noTextLinks = false) shouldEqual expectedLinks
	}

	"Entry with extended Links" should "be exactly this entry" in {
		val job = new LinkExtender
		job.parsedWikipedia = sc.parallelize(List(TestData.linkExtenderParsedEntry()))
		job.pages = sc.parallelize(TestData.linkExtenderPagesSet().toList)
		job.run(sc)
		val extendedEntries = job
			.extendedParsedWikipedia
			.collect
			.toSet
		extendedEntries shouldEqual TestData.linkExtenderExtendedParsedEntry()
	}

	"Big Entry with extended Links" should "be exactly this big entry" in {
		val job = new LinkExtender
		job.parsedWikipedia = sc.parallelize(List(TestData.bigLinkExtenderParsedEntry()))
		job.pages = sc.parallelize(TestData.bigLinkExtenderPagesSet().toList)
		job.run(sc)
		val extendedEntry = job
			.extendedParsedWikipedia
			.collect
			.toSet
			.head
		val expectedLinks = TestData.bigLinkExtenderExtendedParsedEntry().head.extendedlinks(noTextLinks = false)
		extendedEntry.extendedlinks(noTextLinks = false) shouldEqual expectedLinks
	}
}
