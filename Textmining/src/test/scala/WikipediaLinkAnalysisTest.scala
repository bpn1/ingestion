import com.holdenkarau.spark.testing.{SharedSparkContext, RDDComparisons}
import org.scalatest.FlatSpec
import org.apache.spark.rdd._
import WikiClasses._

class WikipediaLinkAnalysisTest extends FlatSpec with SharedSparkContext with RDDComparisons {
	"Grouped aliases" should "not be empty" in {
		val groupedAliases = WikipediaLinkAnalysis
			.groupByAliases(TestData.smallerParsedWikipediaTestRDD(sc))
		assert(groupedAliases.count > 0)
	}

	they should "be exactly these aliases" in {
		val groupedAliases = WikipediaLinkAnalysis
			.groupByAliases(TestData.smallerParsedWikipediaTestRDD(sc))
			.map(alias => (alias.alias, alias.pages.toMap))
		val groupedAliasesTest = TestData.groupedAliasesTestRDD(sc)
			.map(alias => (alias.alias, alias.pages.toMap))
		assertRDDEquals(groupedAliases, groupedAliasesTest)
	}

	"Grouped page names" should "not be empty" in {
		val groupedPageNames = WikipediaLinkAnalysis
			.groupByPageNames(TestData.smallerParsedWikipediaTestRDD(sc))
		assert(groupedPageNames.count > 0)
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
