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
import de.hpi.ingestion.textmining.models.{Link, ParsedWikipediaEntry}
import de.hpi.ingestion.textmining.preprocessing.TextParser.disambiguationTitleSuffix
import org.jsoup.Jsoup
import org.scalatest.{FlatSpec, _}

import scala.util.matching.Regex

class TextParserTest extends FlatSpec with SharedSparkContext with Matchers {
	"Wikipedia entry title" should "not change" in {
		TestData.wikipediaEntries()
			.map(entry =>
				(entry.title, TextParser.wikipediaToHtml(entry.getText())))
			.map(tuple => (tuple._1, TextParser.parseHtml(tuple._1, tuple._2).title))
			.foreach(entry => entry._1 shouldEqual entry._2)
	}

	"Wikipedia article text" should "not contain wikitext" in {
		// matches [[...]] and {{...}} but not escaped '{', i.e. "\{"
		val wikitextRegex = new Regex("(\\[\\[.*?\\]\\])" + "|" + "([^\\\\]\\{\\{.*?\\}\\})")
		TestData.wikipediaEntries()
			.map(entry => TextParser.wikipediaToHtml(entry.getText()))
			.foreach(element => wikitextRegex.findFirstIn(element) shouldBe empty)
	}

	it should "not contain escaped HTML characters" in {
		val wikitextRegex = new Regex("&\\S*?;")
		TestData.wikipediaEntries()
			.map { entry =>
				val html = TextParser.wikipediaToHtml(entry.getText())
				TextParser.parseHtml(entry.title, html)
			}
			.foreach(element => wikitextRegex.findFirstIn(element.getText()) shouldBe empty)
	}

	it should "contain none of the tags: table, h[0-9], small" in {
		val tagBlacklist = List[String]("table", "h[0-9]", "small")
		TestData.wikipediaEntries()
			.map { entry =>
				val html = TextParser.wikipediaToHtml(entry.getText())
				TextParser.parseHtml(entry.title, html).getText()
			}
			.foreach { element =>
				for(tag <- tagBlacklist) {
					val tagRegex = new Regex("(</" + tag + ">)|(<" + tag + "(>| .*?>))")
					tagRegex.findFirstIn(element) shouldBe empty
				}
			}
	}

	it should "parse and return all text properly" in {
		val abstracts = TestData.wikipediaAbstracts()
		TestData.wikipediaEntries()
			.map { entry =>
				val html = TextParser.wikipediaToHtml(entry.getText())
				TextParser.parseHtml(entry.title, html)
			}
			.filter(entry => abstracts.contains(entry.title))
			.foreach(element => element.getText() should startWith(abstracts(element.title)))
	}

	"Wikipedia text links" should "not be empty" in {
		TestData.wikipediaEntries()
			.map(TextParser.parseWikipediaEntry)
			.filter(_.disambiguationlinks.isEmpty)
			.map(_.textlinks)
			.foreach(textLinks => textLinks should not be empty)
	}

	they should "be valid (have alias and page)" in {
		TestData.wikipediaEntries()
			.map(TextParser.parseWikipediaEntry)
			.flatMap(_.textlinks)
			.foreach(link => assert(isLinkValid(link)))
	}

	they should "be exactly these links" in {
		val textLinks = TestData.wikipediaTextLinks()
		TestData.wikipediaEntries()
			.map(TextParser.parseWikipediaEntry)
			.filter(entry => textLinks.contains(entry.title))
			.foreach(entry => entry.textlinks shouldEqual textLinks(entry.title))
	}

	they should "be extracted" in {
		val linkTuples = TestData.textLinkHtml().map(TextParser.extractTextAndTextLinks)
		val expectedTuples = TestData.textLinkTuples()
		linkTuples shouldEqual expectedTuples
	}

	"Wikipedia text link offsets" should "be consistent with text" in {
		TestData.wikipediaEntries()
			.map(TextParser.parseWikipediaEntry)
			.filter(!_.getText().startsWith(TextParser.redirectText))
			.foreach { element =>
				element.textlinks.foreach { link =>
					if(!isTextLinkConsistent(link, element.getText())) {
						println(link)
						println(element.getText())
					}
					assert(isTextLinkConsistent(link, element.getText()))
				}
			}
	}

	"List links" should "be extracted" in {
		val testLinks = TextParser.extractListLinks(TestData.htmlListLinkPage())
		val expectedLinks = TestData.extractedListLinksList()
		testLinks shouldBe expectedLinks
	}

	"All redirects" should "contain #WEITERLEITUNG link" in {
		val entries = TestData.entriesWithBadRedirectsList()
		entries.map(entry => TextParser.cleanRedirectsAndWhitespaces(entry))
			.map(entry => entry.getText should startWith(s"#${TextParser.parsableRedirect}"))
	}

	"Wikipedia disambiguation pages" should "be recognized as such" in {
		val disambiguationPages = TestData.wikipediaEntries()
			.filter(TextParser.isDisambiguationPage)
			.map(_.title)
			.toSet
		disambiguationPages shouldEqual TestData.wikipediaDisambiguationPagesSet
	}

	"Wikipedia disambiguation links" should "be found" in {
		val job = new TextParser
		job.wikipedia = sc.parallelize(TestData.wikipediaEntries())
			.filter(entry => TestData.wikipediaDisambiguationPagesSet().contains(entry.title))
		job.run(sc)
		job
			.parsedWikipedia
			.collect
			.foreach { entry =>
				entry.disambiguationlinks should not be empty
			}
	}

	they should "direct from the page name itself (without '" +
		TextParser.disambiguationTitleSuffix + "') to another page" in {
		val job = new TextParser
		job.wikipedia = sc.parallelize(TestData.wikipediaEntries())
			.filter(entry => TestData.wikipediaDisambiguationPagesSet().contains(entry.title))
		job.run(sc)
		job
			.parsedWikipedia
			.collect
			.foreach { entry =>
				entry.disambiguationlinks.foreach { link =>
					val cleanTitle = entry.title.stripSuffix(disambiguationTitleSuffix)
					link.alias shouldEqual cleanTitle
					link.page should not equal entry.title
				}
			}
	}

	"Template links" should "not be empty" in {
		val job = new TextParser
		val templateArticlesTest = TestData.wikipediaTemplateArticles()
		job.wikipedia = sc.parallelize(TestData.wikipediaEntries())
			.filter(entry => templateArticlesTest.contains(entry.title))
		job.run(sc)
		job
			.parsedWikipedia
			.map(_.templatelinks)
			.collect
			.foreach(templateLinks => templateLinks should not be empty)
	}

	they should "be valid (have alias and page)" in {
		TestData.wikipediaEntries()
			.map { entry =>
				val html = TextParser.wikipediaToHtml(entry.getText())
				TextParser.parseHtml(entry.title, html)
			}.flatMap(_.templatelinks)
			.foreach(link => assert(isLinkValid(link)))
	}

	they should "be exactly these links" in {
		val job = new TextParser
		job.wikipedia = sc.parallelize(TestData.wikipediaEntries())
		job.run(sc)
		val templateLinks = TestData.wikipediaTemplateLinks()
		job
			.parsedWikipedia
			.filter(entry => templateLinks.contains(entry.title))
			.collect
			.foreach(entry => entry.templatelinks shouldEqual templateLinks(entry.title))
	}

	"Bad namespace pages" should "be filtered" in {
		val testPages = TestData.namespacePagesList()
			.filterNot(TextParser.isMetaPage)
		val expectedPages = TestData.cleanedNamespacePagesList()
		testPages shouldEqual expectedPages

	}

	"Bad namespace links" should "be filtered" in {
		val testLinks = TextParser
			.cleanMetapageLinks(TestData.namespaceLinksList())
		val expectedLinks = TestData.cleanedNamespaceLinksList()
		testLinks shouldEqual expectedLinks
	}

	"Category links" should "be extracted" in {
		val testLinks = TestData.categoryLinksList()
		val testEntry = ParsedWikipediaEntry(title = "test", textlinks = testLinks)
		val result = TextParser.extractCategoryLinks(testEntry)
		val expectedLinks = TestData.cleanedCategoryLinksList()
		val expectedCategoryLinks = TestData.extractedCategoryLinksList()
		result.textlinks shouldEqual expectedLinks
		result.categorylinks shouldEqual expectedCategoryLinks
	}

	they should "have cleaned aliases and pages" in {
		val testLinks = TestData.categoryLinksList()
		val testEntry = ParsedWikipediaEntry(title = "test", textlinks = testLinks)
		TextParser
			.extractCategoryLinks(testEntry)
			.categorylinks
			.foreach { link =>
				link.alias shouldNot startWith(TextParser.categoryNamespace)
				link.page shouldNot startWith(TextParser.categoryNamespace)
			}
	}

	they should "have their offset set to the default value" in {
		val testLinks = TestData.categoryLinksList()
		val testEntry = ParsedWikipediaEntry(title = "test", textlinks = testLinks)
		val defaultOffset = Link("", "").offset
		TextParser
			.extractCategoryLinks(testEntry)
			.categorylinks
			.foreach(link => link.offset shouldBe defaultOffset)
	}

	"Cleaned Wikipedia document" should "not contain other tags than anchors" in {
		List(TestData.document())
			.map(Jsoup.parse)
			.map(TextParser.removeTags)
			.map(_.toString)
			.foreach(document => document should (not include "</span>"
				and not include "</p>" and not include "</abbr>"))
	}

	they should "contain headlines" in {
		List(TestData.documentWithHeadlines())
			.map(Jsoup.parse)
			.map(TextParser.removeTags)
			.map(_.body.text)
			.foreach { text =>
				TestData.wikipediaEntryHeadlines()
					.foreach(headline => assert(text.contains(headline)))
			}
	}

	it should "have exactly this text" in {
		List(TestData.documentWithHeadlines())
			.map(Jsoup.parse)
			.map(TextParser.removeTags)
			.map(_.body.text)
			.foreach(_ shouldEqual TestData.parsedArticleTextWithHeadlines())
	}

	"Redirect page entries" should "be identified as such" in {
		val entries = TestData.parsedEntriesWithRedirects()
			.filter(TextParser.isRedirectPage)
		val expectedEntries = TestData.parsedRedirectEntries()
		entries shouldEqual expectedEntries
	}

	"Wikipedia articles" should "be parsed" in {
		val job = new TextParser
		job.wikipedia = sc.parallelize(TestData.wikipediaEntriesForParsing())
		job.run(sc)
		val parsedArticles = job
			.parsedWikipedia
			.collect
			.toList
		val expectedArticles = TestData.parsedWikipediaEntries()
		parsedArticles shouldEqual expectedArticles
	}

	"Redirects" should "be extracted" in {
		val redirectLinks = TestData.redirectHTML().map((TextParser.extractRedirect _).tupled)
		val expectedLinks = TestData.extractedRedirects()
		redirectLinks shouldEqual expectedLinks
	}

	"Parsed Wikipedia article" should "contain headlines" in {
		val job = new TextParser
		job.wikipedia = sc.parallelize(List(TestData.wikipediaEntryWithHeadlines()))
		job.run(sc)
		val parsedArticleText = job
			.parsedWikipedia
			.collect
			.head
			.getText()

		TestData.wikipediaEntryHeadlines()
			.foreach(headline => assert(parsedArticleText.contains(headline)))
	}

	it should "have exactly this text" in {
		val job = new TextParser
		job.wikipedia = sc.parallelize(List(TestData.wikipediaEntryWithHeadlines()))
		job.run(sc)
		val parsedArticleText = job
			.parsedWikipedia
			.collect
			.head
			.getText()
		parsedArticleText shouldEqual TestData.parsedArticleTextWithHeadlines()
	}

	"Alternative whitespace characters" should "be replaced with standard whitespaces" in {
		val entry = TestData.entryWithAlternativeWhitespace()
		val cleanedEntry = TextParser.cleanRedirectsAndWhitespaces(entry)
		cleanedEntry shouldEqual TestData.entryWithStandardWhitespaces()
	}

	they should "be replaced" in {
		val cleanedEntry = TextParser.cleanRedirectsAndWhitespaces(TestData.rawEntryWithAlternativeWhitespace())
		val expectedEntry = TestData.rawEntryWithStandardWhitespace()
		cleanedEntry shouldEqual expectedEntry
	}

	def isTextLinkConsistent(link: Link, text: String): Boolean = {
		val substring = text.substring(link.offset.get, link.offset.get + link.alias.length)
		substring == link.alias
	}

	def isLinkValid(link: Link): Boolean = {
		val result = link.alias.nonEmpty && link.page.nonEmpty
		if(!result) {
			println(link)
		}
		result
	}
}
