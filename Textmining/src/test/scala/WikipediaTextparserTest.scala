import WikiClasses._
import WikipediaTextparser.disambiguationTitleSuffix
import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.SharedSparkContext
import scala.util.matching.Regex
import org.scalatest._

class WikipediaTextparserTest extends FlatSpec with SharedSparkContext with Matchers {
	"Wikipedia entry title" should "not change" in {
		TestData.wikipediaEntriesTestList()
			.map(entry =>
				(entry.title, WikipediaTextparser.wikipediaToHtml(entry.getText())))
			.map(tuple => (tuple._1, WikipediaTextparser.parseHtml(tuple._1, tuple._2).title))
			.foreach(entry => entry._1 shouldEqual entry._2)
	}

	"Wikipedia article text" should "not contain Wikimarkup" in {
		// matches [[...]] and {{...}} but not escaped '{', i.e. "\{"
		val wikimarkupRegex = new Regex("(\\[\\[.*?\\]\\])" + "|" + "([^\\\\]\\{\\{.*?\\}\\})")
		TestData.wikipediaEntriesTestList()
			.map(entry => WikipediaTextparser.wikipediaToHtml(entry.getText()))
			.foreach(element => wikimarkupRegex.findFirstIn(element) shouldBe empty)
	}

	it should "not contain escaped HTML characters" in {
		val wikimarkupRegex = new Regex("&\\S*?;")
		TestData.wikipediaEntriesTestList()
			.map { entry =>
				val html = WikipediaTextparser.wikipediaToHtml(entry.getText())
				WikipediaTextparser.parseHtml(entry.title, html)
			}
			.foreach(element => wikimarkupRegex.findFirstIn(element.getText()) shouldBe empty)
	}

	it should "contain none of the tags: table, h[0-9], small" in {
		val tagBlacklist = List[String]("table", "h[0-9]", "small")
		TestData.wikipediaEntriesTestList()
			.map { entry =>
				val html = WikipediaTextparser.wikipediaToHtml(entry.getText())
				WikipediaTextparser.parseHtml(entry.title, html).getText()
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
				val html = WikipediaTextparser.wikipediaToHtml(entry.getText())
				WikipediaTextparser.parseHtml(entry.title, html)
			}
			.filter(entry => abstracts.contains(entry.title))
			.foreach(element => element.getText() should startWith(abstracts(element.title)))
	}

	"Wikipedia links" should "not be empty" in {
		TestData.wikipediaEntriesTestList()
			.map(WikipediaTextparser.parseWikipediaEntry)
			.map(_.links)
			.foreach(links => links should not be empty)
	}

	they should "contain links from templates in article" in {
		val templateArticlesTest = TestData.wikipediaTestTemplateArticles()
		TestData.wikipediaEntriesTestList()
			.filter(entry => templateArticlesTest.contains(entry.title))
			.map(WikipediaTextparser.parseWikipediaEntry)
			.map(_.links)
			.foreach(links =>
				assert(links.exists(link => link.offset == WikipediaTextparser.templateOffset)))
	}

	they should "be valid (have alias and page)" in {
		TestData.wikipediaEntriesTestList()
			.flatMap { entry =>
				val html = WikipediaTextparser.wikipediaToHtml(entry.getText())
				WikipediaTextparser.parseHtml(entry.title, html).links
			}
			.foreach { link =>
				link.alias should not be empty
				link.page should not be empty
			}
	}

	they should "be exactly these links" in {
		val links = TestData.wikipediaTestReferences()
		TestData.wikipediaEntriesTestList()
			.map(WikipediaTextparser.parseWikipediaEntry)
			.filter(entry => links.contains(entry.title))
			.foreach(entry => entry.links shouldEqual links(entry.title))
	}

	"Wikipedia text link offsets" should "be consistent with text" in {
		TestData.wikipediaEntriesTestList()
			.map { entry =>
				val html = WikipediaTextparser.wikipediaToHtml(entry.getText())
				WikipediaTextparser.parseHtml(entry.title, html)
			}
			.foreach { element =>
				element.links.foreach { link =>
					if (!isTemplateLink(link)) {
						if (!isTextLinkConsistent(link, element.getText())) {
							println(link)
							println(element.getText())
						}
						assert(isTextLinkConsistent(link, element.getText()))
					}
				}
			}
	}

	"Wikipedia disambiguation pages" should "be recognized as such" in {
		val disambiguationPages = TestData.wikipediaEntriesTestList()
			.filter(entry => WikipediaTextparser.isDisambiguationPage(entry))
			.map(_.title)
			.toSet
		disambiguationPages shouldEqual TestData.wikipediaDisambiguationPagesTestSet
	}

	"Wikipedia disambiguation links" should "direct from the page name itself (without '" +
		WikipediaTextparser.disambiguationTitleSuffix + "') to another page" in {
		TestData.wikipediaEntriesTestList()
			.filter(entry => TestData.wikipediaDisambiguationPagesTestSet().contains(entry.title))
			.map(WikipediaTextparser.parseWikipediaEntry)
			.foreach { entry =>
				entry.links
					.foreach { link =>
						val cleanTitle = entry.title.stripSuffix(disambiguationTitleSuffix)
						link.alias shouldEqual cleanTitle
						link.page should not equal entry.title
					}
			}
	}

	"Category links" should "be extracted" in {
		val testLinks = TestData.testCategoryLinks()
		val testEntry = ParsedWikipediaEntry(title = "test", links = testLinks)
		val result = WikipediaTextparser.extractCategoryLinks(testEntry)
		val expectedLinks = TestData.testCleanedCategoryLinks()
		val expectedCategoryLinks = TestData.testExtractedCategoryLinks()
		result.links shouldEqual expectedLinks
		result.category_links shouldEqual expectedCategoryLinks
	}

	they should "have cleaned aliases and pages" in {
		val testLinks = TestData.testCategoryLinks()
		val testEntry = ParsedWikipediaEntry(title = "test", links = testLinks)
		WikipediaTextparser
			.extractCategoryLinks(testEntry)
			.category_links
			.foreach { link =>
				link.alias shouldNot startWith (WikipediaTextparser.categoryNamespace)
				link.page shouldNot startWith (WikipediaTextparser.categoryNamespace)
			}
	}

	they should "have their offset set to zero" in {
		val testLinks = TestData.testCategoryLinks()
		val testEntry = ParsedWikipediaEntry(title = "test", links = testLinks)
		WikipediaTextparser
			.extractCategoryLinks(testEntry)
			.category_links
			.foreach { link => link.offset shouldBe 0
			}
	}

	def isTextLinkConsistent(link: Link, text: String): Boolean = {
		val substring = text.substring(link.offset, link.offset + link.alias.length)
		substring == link.alias
	}

	def isTemplateLink(link: Link): Boolean = {
		link.offset == WikipediaTextparser.templateOffset
	}

	def isLinkValid(link: Link): Boolean = {
		val result = link.alias.nonEmpty && link.page.nonEmpty
		if (!result) {
			println(link)
		}
		result
	}
}
