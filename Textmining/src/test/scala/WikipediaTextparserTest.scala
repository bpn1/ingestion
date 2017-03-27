import WikiClasses._
import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.SharedSparkContext
import scala.util.matching.Regex
import org.scalatest._

class WikipediaTextparserTest extends FlatSpec with SharedSparkContext with Matchers {
	"Wikipedia entry title" should "not change" in {
		TestData.wikipediaTestRDD(sc)
			.map(entry =>
				(entry.title, (entry, WikipediaTextparser.wikipediaToHtml(entry.getText()))))
			.map(tuple => (tuple._1, WikipediaTextparser.parseHtml(tuple._2).title))
			.collect
			.foreach(entry => entry._1 shouldEqual entry._2)
	}

	"Wikipedia article text" should "not contain Wikimarkup" in {
		// matches [[...]] and {{...}} but not escaped '{', i.e. "\{"
		val wikimarkupRegex = new Regex("(\\[\\[.*?\\]\\])" + "|" + "([^\\\\]\\{\\{.*?\\}\\})")
		TestData.wikipediaTestRDD(sc)
			.map(entry => WikipediaTextparser.wikipediaToHtml(entry.getText()))
			.collect
			.foreach(element => wikimarkupRegex.findFirstIn(element) shouldBe empty)
	}

	it should "not contain escaped HTML characters" in {
		val wikimarkupRegex = new Regex("&\\S*?;")
		TestData.wikipediaTestRDD(sc)
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText())))
			.map(WikipediaTextparser.parseHtml)
			.collect
			.foreach(element => wikimarkupRegex.findFirstIn(element.getText()) shouldBe empty)
	}

	it should "contain none of the tags: table, h[0-9], small" in {
		val tagBlacklist = List[String]("table", "h[0-9]", "small")
		TestData.wikipediaTestRDD(sc)
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText())))
			.map(WikipediaTextparser.parseHtml)
			.map(_.getText())
			.collect
			.foreach { element =>
				for (tag <- tagBlacklist) {
					val tagRegex = new Regex("(</" + tag + ">)|(<" + tag + "(>| .*?>))")
					tagRegex.findFirstIn(element) shouldBe empty
				}
			}
	}

	it should "parse and return all text properly" in {
		val abstracts = TestData.wikipediaTestAbstracts()
		TestData.wikipediaTestRDD(sc)
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText())))
			.map(WikipediaTextparser.parseHtml)
			.filter(entry => abstracts.contains(entry.title))
			.collect
			.foreach(element => element.getText() should startWith(abstracts(element.title)))
	}

	"Wikipedia links" should "not be empty" in {
		TestData.wikipediaTestRDD(sc)
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText())))
			.map(WikipediaTextparser.parseHtml)
			.map(_.links)
			.collect
			.foreach(links => links should not be empty)
	}

	they should "contain links from templates in article" in {
		val templateArticlesTest = TestData.wikipediaTestTemplateArticles()
		TestData.wikipediaTestRDD(sc)
			.filter(entry => templateArticlesTest.contains(entry.title))
			.map(WikipediaTextparser.parseWikipediaEntry)
			.map(_.links)
			.collect
			.foreach(links =>
				assert(links.exists(link => link.offset == WikipediaTextparser.templateOffset)))
	}

	they should "be valid (have alias and page)" in {
		TestData.wikipediaTestRDD(sc)
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText())))
			.map(WikipediaTextparser.parseHtml)
			.flatMap(_.links)
			.collect
			.foreach(link => assert(isLinkValid(link)))
	}

	they should "be exactly these links" in {
		val links = TestData.wikipediaTestReferences()
		TestData.wikipediaTestRDD(sc)
			.map(WikipediaTextparser.parseWikipediaEntry)
			.filter(entry => links.contains(entry.title))
			.collect
			.foreach(entry => entry.links shouldEqual links(entry.title))
	}

	"Wikipedia text link offsets" should "be consistent with text" in {
		TestData.wikipediaTestRDD(sc)
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText())))
			.map(WikipediaTextparser.parseHtml)
			.collect
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
			.foreach { link =>
				link.offset shouldBe 0
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
