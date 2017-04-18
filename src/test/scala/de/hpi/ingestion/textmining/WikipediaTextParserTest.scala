package de.hpi.ingestion.textmining

import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.textmining.models.{Link, ParsedWikipediaEntry}
import de.hpi.ingestion.textmining.WikipediaTextParser.disambiguationTitleSuffix

import scala.util.matching.Regex
import org.scalatest._
import org.jsoup.Jsoup

class WikipediaTextParserTest extends FlatSpec with SharedSparkContext with Matchers {
	"Wikipedia entry title" should "not change" in {
		TestData.wikipediaEntriesTestList()
			.map(entry =>
				(entry.title, WikipediaTextParser.wikipediaToHtml(entry.getText())))
			.map(tuple => (tuple._1, WikipediaTextParser.parseHtml(tuple._1, tuple._2).title))
			.foreach(entry => entry._1 shouldEqual entry._2)
	}

	"Wikipedia article text" should "not contain Wikimarkup" in {
		// matches [[...]] and {{...}} but not escaped '{', i.e. "\{"
		val wikimarkupRegex = new Regex("(\\[\\[.*?\\]\\])" + "|" + "([^\\\\]\\{\\{.*?\\}\\})")
		TestData.wikipediaEntriesTestList()
			.map(entry => WikipediaTextParser.wikipediaToHtml(entry.getText()))
			.foreach(element => wikimarkupRegex.findFirstIn(element) shouldBe empty)
	}

	it should "not contain escaped HTML characters" in {
		val wikimarkupRegex = new Regex("&\\S*?;")
		TestData.wikipediaEntriesTestList()
			.map { entry =>
				val html = WikipediaTextParser.wikipediaToHtml(entry.getText())
				WikipediaTextParser.parseHtml(entry.title, html)
			}
			.foreach(element => wikimarkupRegex.findFirstIn(element.getText()) shouldBe empty)
	}

	it should "contain none of the tags: table, h[0-9], small" in {
		val tagBlacklist = List[String]("table", "h[0-9]", "small")
		TestData.wikipediaEntriesTestList()
			.map { entry =>
				val html = WikipediaTextParser.wikipediaToHtml(entry.getText())
				WikipediaTextParser.parseHtml(entry.title, html).getText()
			}
			.foreach { element =>
				for (tag <- tagBlacklist) {
					val tagRegex = new Regex("(</" + tag + ">)|(<" + tag + "(>| .*?>))")
					tagRegex.findFirstIn(element) shouldBe empty
				}
			}
	}

	it should "parse and return all text properly" in {
		val abstracts = TestData.wikipediaTestAbstracts()
		TestData.wikipediaEntriesTestList()
			.map { entry =>
				val html = WikipediaTextParser.wikipediaToHtml(entry.getText())
				WikipediaTextParser.parseHtml(entry.title, html)
			}
			.filter(entry => abstracts.contains(entry.title))
			.foreach(element => element.getText() should startWith(abstracts(element.title)))
	}

	"Wikipedia text links" should "not be empty" in {
		TestData.wikipediaEntriesTestList()
			.map(WikipediaTextParser.parseWikipediaEntry)
			.filter(_.disambiguationlinks.isEmpty)
			.map(_.textlinks)
			.foreach(textLinks => textLinks should not be empty)
	}

	they should "be valid (have alias and page)" in {
		TestData.wikipediaEntriesTestList()
			.map(WikipediaTextParser.parseWikipediaEntry)
			.flatMap(_.textlinks)
			.foreach(link => assert(isLinkValid(link)))
	}

	they should "be exactly these links" in {
		val textLinks = TestData.wikipediaTestTextLinks()
		TestData.wikipediaEntriesTestList()
			.map(WikipediaTextParser.parseWikipediaEntry)
			.filter(entry => textLinks.contains(entry.title))
			.foreach(entry => entry.textlinks shouldEqual textLinks(entry.title))
	}

	"Wikipedia text link offsets" should "be consistent with text" in {
		TestData.wikipediaEntriesTestList()
			.map(WikipediaTextParser.parseWikipediaEntry)
			.filter(!_.getText.startsWith("REDIRECT"))
			.foreach { element =>
				element.textlinks.foreach { link =>
					if (!isTextLinkConsistent(link, element.getText())) {
						println(link)
						println(element.getText())
					}
					assert(isTextLinkConsistent(link, element.getText()))
				}
			}
	}

	"List links" should "be extracted" in {
		val testLinks = WikipediaTextParser.extractListLinks(TestData.testListLinkPage())
		val expectedLinks = TestData.testExtractedListLinks()
		testLinks shouldBe expectedLinks
	}

	"All redirects" should "contain WEITERLEITUNG link" in {
		val entries = TestData.testEntriesWithBadRedirects()
		entries.map(entry => WikipediaTextParser.cleanRedirects(entry))
			.map(entry => entry.getText should startWith("WEITERLEITUNG"))
	}

	"Wikipedia disambiguation pages" should "be recognized as such" in {
		val disambiguationPages = TestData.wikipediaEntriesTestList()
			.filter(WikipediaTextParser.isDisambiguationPage)
			.map(_.title)
			.toSet
		disambiguationPages shouldEqual TestData.wikipediaDisambiguationPagesTestSet
	}

	"Wikipedia disambiguation links" should "be found" in {
		TestData.wikipediaEntriesTestList()
			.filter(entry => TestData.wikipediaDisambiguationPagesTestSet().contains(entry.title))
			.map(WikipediaTextParser.parseWikipediaEntry)
			.foreach { entry =>
				entry.disambiguationlinks should not be empty
			}
	}

	they should "direct from the page name itself (without '" +
		WikipediaTextParser.disambiguationTitleSuffix + "') to another page" in {
		TestData.wikipediaEntriesTestList()
			.filter(entry => TestData.wikipediaDisambiguationPagesTestSet().contains(entry.title))
			.map(WikipediaTextParser.parseWikipediaEntry)
			.foreach { entry =>
				entry.disambiguationlinks.foreach { link =>
					val cleanTitle = entry.title.stripSuffix(disambiguationTitleSuffix)
					link.alias shouldEqual cleanTitle
					link.page should not equal entry.title
				}
			}
	}

	"Template links" should "not be empty" in {
		val templateArticlesTest = TestData.wikipediaTestTemplateArticles()
		TestData.wikipediaEntriesTestList()
			.filter(entry => templateArticlesTest.contains(entry.title))
			.map(WikipediaTextParser.parseWikipediaEntry)
			.map(_.templatelinks)
			.foreach(templateLinks => templateLinks should not be empty)
	}

	they should "be valid (have alias and page)" in {
		TestData.wikipediaEntriesTestList()
			.map { entry =>
				val html = WikipediaTextParser.wikipediaToHtml(entry.getText())
				WikipediaTextParser.parseHtml(entry.title, html)
			}.flatMap(_.templatelinks)
			.foreach(link => assert(isLinkValid(link)))
	}

	they should "be exactly these links" in {
		val templateLinks = TestData.wikipediaTestTemplateLinks()
		TestData.wikipediaEntriesTestList()
			.map(WikipediaTextParser.parseWikipediaEntry)
			.filter(entry => templateLinks.contains(entry.title))
			.foreach(entry =>
				entry.templatelinks shouldEqual templateLinks(entry.title))
	}

	"Bad namespace pages" should "be filtered" in {
		val testPages = TestData.testNamespacePages()
			.filterNot(WikipediaTextParser.isMetaPage)
		val expectedPages = TestData.testCleanedNamespacePages()
		testPages shouldEqual expectedPages

	}

	"Bad namespace links" should "be filtered" in {
		val testLinks = WikipediaTextParser.cleanMetapageLinks(TestData.testNamespaceLinks())
		val expectedLinks = TestData.testCleanedNamespaceLinks()
		testLinks shouldEqual expectedLinks
	}

	"Category links" should "be extracted" in {
		val testLinks = TestData.testCategoryLinks()
		val testEntry = ParsedWikipediaEntry(title = "test", textlinks = testLinks)
		val result = WikipediaTextParser.extractCategoryLinks(testEntry)
		val expectedLinks = TestData.testCleanedCategoryLinks()
		val expectedCategoryLinks = TestData.testExtractedCategoryLinks()
		result.textlinks shouldEqual expectedLinks
		result.categorylinks shouldEqual expectedCategoryLinks
	}

	they should "have cleaned aliases and pages" in {
		val testLinks = TestData.testCategoryLinks()
		val testEntry = ParsedWikipediaEntry(title = "test", textlinks = testLinks)
		WikipediaTextParser
			.extractCategoryLinks(testEntry)
			.categorylinks
			.foreach { link =>
				link.alias shouldNot startWith(WikipediaTextParser.categoryNamespace)
				link.page shouldNot startWith(WikipediaTextParser.categoryNamespace)
			}
	}

	they should "have their offset set to zero" in {
		val testLinks = TestData.testCategoryLinks()
		val testEntry = ParsedWikipediaEntry(title = "test", textlinks = testLinks)
		WikipediaTextParser
			.extractCategoryLinks(testEntry)
			.categorylinks
			.foreach(link => link.offset shouldBe 0)
	}

	"Cleaned Wikipedia document" should "not contain other tags than anchors" in {
		TestData.documentTestList()
			.map(Jsoup.parse)
			.map(WikipediaTextParser.removeTags)
			.map(_.toString)
			.foreach(document => document should (not include "</span>"
				and not include "</p>" and not include "</abbr>"))
	}

	def isTextLinkConsistent(link: Link, text: String): Boolean = {
		val substring = text.substring(link.offset, link.offset + link.alias.length)
		substring == link.alias
	}

	def isLinkValid(link: Link): Boolean = {
		val result = link.alias.nonEmpty && link.page.nonEmpty
		if (!result) {
			println(link)
		}
		result
	}
}
