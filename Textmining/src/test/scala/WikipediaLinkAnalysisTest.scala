import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class WikipediaLinkAnalysisTest extends FlatSpec with SharedSparkContext with Matchers {
	"Grouped aliases" should "not be empty" in {
		val groupedAliases = WikipediaLinkAnalysis
			.groupByAliases(sc.parallelize(TestData.smallerParsedWikipediaTestList()))
			.collect
			.toSet
		groupedAliases should not be empty
	}

	they should "be exactly these aliases" in {
		val groupedAliases = WikipediaLinkAnalysis
			.groupByAliases(sc.parallelize(TestData.smallerParsedWikipediaTestList()))
			.map(alias => (alias.alias, alias.pages))
			.collect
			.toSet
		val groupedAliasesTest = TestData.groupedAliasesTestSet()
			.map(alias => (alias.alias, alias.pages))
		groupedAliases shouldEqual groupedAliasesTest
	}

	"Grouped page names" should "not be empty" in {
		val groupedPageNames = WikipediaLinkAnalysis
			.groupByPageNames(sc.parallelize(TestData.smallerParsedWikipediaTestList()))
			.collect
			.toSet
		groupedPageNames should not be empty
	}

	they should "be exactly these page names" in {
		val groupedPages = WikipediaLinkAnalysis
			.groupByPageNames(sc.parallelize(TestData.smallerParsedWikipediaTestList()))
			.collect
			.toSet
		groupedPages shouldEqual TestData.groupedPagesTestSet()
	}

	"Probability that link directs to page" should "be computed correctly" in {
		val references = TestData.probabilityReferences()
		TestData.cleanedGroupedAliasesTestSet()
			.map(link =>
				(link.alias, WikipediaLinkAnalysis.probabilityLinkDirectsToPage(link, "Bayern")))
			.foreach { case (alias, probability) => probability shouldEqual references(alias) }
	}

	"Dead links and only dead links" should "be removed" in {
		val cleanedGroupedAliases = WikipediaLinkAnalysis.removeDeadLinks(
			sc.parallelize(TestData.groupedAliasesTestSet().toList),
			sc.parallelize(TestData.allPagesTestList()))
			.collect
			.toSet
		cleanedGroupedAliases shouldEqual TestData.cleanedGroupedAliasesTestSet()
	}

	"Dead pages and only dead pages" should "be removed" in {
		val cleanedGroupedPages = WikipediaLinkAnalysis.removeDeadPages(
			sc.parallelize(TestData.groupedPagesTestSet().toList),
			sc.parallelize(TestData.allPagesTestList()))
			.collect
			.toSet
		cleanedGroupedPages shouldEqual TestData.cleanedGroupedPagesTestSet()
	}
}
