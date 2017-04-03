import com.holdenkarau.spark.testing.{SharedSparkContext, RDDComparisons}
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.rdd._
import WikiClasses._

class WikipediaLinkAnalysisTest extends FlatSpec with SharedSparkContext with RDDComparisons
	with Matchers {
	"Page names grouped by aliases" should "not be empty" in {
		val groupedAliases = WikipediaLinkAnalysis
			.groupByAliases(TestData.smallerParsedWikipediaTestRDD(sc))
		assert(groupedAliases.count > 0)
	}

	they should "not have empty aliases" in {
		WikipediaLinkAnalysis
			.groupByAliases(TestData.smallerParsedWikipediaTestRDD(sc))
			.collect
			.foreach(alias => alias.alias should not be empty)
	}

	they should "contain at least one page name per alias" in {
		WikipediaLinkAnalysis
			.groupByAliases(TestData.smallerParsedWikipediaTestRDD(sc))
			.collect
			.foreach(alias => alias.pages should not be empty)
	}

	they should "not contain empty page names" in {
		val pages = WikipediaLinkAnalysis
			.groupByAliases(TestData.smallerParsedWikipediaTestRDD(sc))
			.flatMap(_.pages)
			.map(_._1)
			.collect
			.foreach(page => page should not be "")
	}

	they should "be exactly these aliases" in {
		val groupedAliases = WikipediaLinkAnalysis
			.groupByAliases(TestData.smallerParsedWikipediaTestRDD(sc))
			.map(alias => (alias.alias, alias.pages.toMap))
			.collect
			.toSet
		val groupedAliasesTest = TestData.groupedAliasesTestRDD(sc)
			.map(alias => (alias.alias, alias.pages.toMap))
			.collect
			.toSet
		groupedAliases shouldEqual groupedAliasesTest
	}

	"Aliases grouped by page names" should "not be empty" in {
		val groupedPageNames = WikipediaLinkAnalysis
			.groupByPageNames(TestData.smallerParsedWikipediaTestRDD(sc))
		assert(groupedPageNames.count > 0)
	}

	they should "not have empty page names" in {
		WikipediaLinkAnalysis
			.groupByPageNames(TestData.smallerParsedWikipediaTestRDD(sc))
			.collect
			.foreach(page => page.page should not be empty)
	}

	they should "contain at least one alias per page name" in {
		WikipediaLinkAnalysis
			.groupByPageNames(TestData.smallerParsedWikipediaTestRDD(sc))
			.collect
			.foreach(pages => pages.aliases should not be empty)
	}

	they should "not contain empty aliases" in {
		val aliases = WikipediaLinkAnalysis
			.groupByPageNames(TestData.smallerParsedWikipediaTestRDD(sc))
			.flatMap(_.aliases)
			.map(_._1)
			.collect
			.foreach(alias => alias should not be "")
	}

	they should "be exactly these page names" in {
		val groupedPages = WikipediaLinkAnalysis
			.groupByPageNames(TestData.smallerParsedWikipediaTestRDD(sc))
		val groupedPagesTest = TestData.groupedPagesTestRDD(sc)
		assertRDDEquals(groupedPages, groupedPagesTest)
	}

	"Probability that link directs to page" should "be computed correctly" in {
		val references = TestData.probabilityReferences()
		TestData.cleanedGroupedAliasesTestRDD(sc)
			.map(link =>
				(link.alias, WikipediaLinkAnalysis.probabilityLinkDirectsToPage(link, "Bayern")))
			.collect
			.foreach { case (alias, probability) => assert(probability == references(alias)) }
	}

	"Dead links and only dead links" should "be removed" in {
		val cleanedGroupedAliases = WikipediaLinkAnalysis.removeDeadLinks(
			TestData.groupedAliasesTestRDD(sc),
			TestData.allPagesTestRDD(sc))
		val cleanedGroupedAliasesTest = TestData.cleanedGroupedAliasesTestRDD(sc)
		assertRDDEquals(cleanedGroupedAliases, cleanedGroupedAliasesTest)
	}

	"Dead pages and only dead pages" should "be removed" in {
		val cleanedGroupedPages = WikipediaLinkAnalysis.removeDeadPages(
			TestData.groupedPagesTestRDD(sc),
			TestData.allPagesTestRDD(sc))
		val cleanedGroupedPagesTest = TestData.cleanedGroupedPagesTestRDD(sc)
		assertRDDEquals(cleanedGroupedPages, cleanedGroupedPagesTest)
	}
}
