package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class LinkAnalysisTest extends FlatSpec with SharedSparkContext with Matchers {
	"Page names grouped by aliases" should "not be empty" in {
		val groupedAliases = LinkAnalysis
			.groupByAliases(sc.parallelize(TestData.smallerParsedWikipediaTestList()))
			.collect
			.toSet
		groupedAliases should not be empty
	}

	they should "not have empty aliases" in {
		LinkAnalysis
			.groupByAliases(sc.parallelize(TestData.smallerParsedWikipediaTestList()))
			.collect
			.foreach(alias => alias.alias should not be empty)
	}

	they should "contain at least one page name per alias" in {
		LinkAnalysis
			.groupByAliases(sc.parallelize(TestData.smallerParsedWikipediaTestList()))
			.collect
			.foreach(alias => alias.pages should not be empty)
	}

	they should "not contain empty page names" in {
		LinkAnalysis
			.groupByAliases(sc.parallelize(TestData.smallerParsedWikipediaTestList()))
			.flatMap(_.pages)
			.map(_._1)
			.collect
			.foreach(page => page should not be "")
	}

	they should "be exactly these aliases" in {
		val groupedAliases = LinkAnalysis
			.groupByAliases(sc.parallelize(TestData.smallerParsedWikipediaTestList()))
			.map(alias => (alias.alias, alias.pages))
			.collect
			.toSet
		val groupedAliasesTest = TestData.groupedAliasesTestSet()
			.map(alias => (alias.alias, alias.pages))
		groupedAliases shouldEqual groupedAliasesTest
	}

	"Aliases grouped by page names" should "not be empty" in {
		val groupedPageNames = LinkAnalysis
			.groupByPageNames(sc.parallelize(TestData.smallerParsedWikipediaTestList()))
			.collect
			.toSet
		groupedPageNames should not be empty
	}

	they should "not have empty page names" in {
		LinkAnalysis
			.groupByPageNames(sc.parallelize(TestData.smallerParsedWikipediaTestList()))
			.collect
			.foreach(page => page.page should not be empty)
	}

	they should "contain at least one alias per page name" in {
		LinkAnalysis
			.groupByPageNames(sc.parallelize(TestData.smallerParsedWikipediaTestList()))
			.collect
			.foreach(pages => pages.aliases should not be empty)
	}

	they should "not contain empty aliases" in {
		LinkAnalysis
			.groupByPageNames(sc.parallelize(TestData.smallerParsedWikipediaTestList()))
			.flatMap(_.aliases)
			.map(_._1)
			.collect
			.foreach(alias => alias should not be "")
	}

	they should "be exactly these page names" in {
		val groupedPages = LinkAnalysis
			.groupByPageNames(sc.parallelize(TestData.smallerParsedWikipediaTestList()))
			.collect
			.toSet
		groupedPages shouldEqual TestData.groupedPagesTestSet()
	}

	"Probability that link directs to page" should "be computed correctly" in {
		val references = TestData.probabilityReferences()
		TestData.cleanedGroupedAliasesTestSet()
			.map(link =>
				(link.alias, LinkAnalysis.probabilityLinkDirectsToPage(link, "Bayern")))
			.foreach { case (alias, probability) => probability shouldEqual references(alias) }
	}

	"Dead links and only dead links" should "be removed" in {
		val cleanedGroupedAliases = LinkAnalysis.removeDeadLinks(
			sc.parallelize(TestData.groupedAliasesTestSet().toList),
			sc.parallelize(TestData.allPagesTestList()))
			.collect
			.toSet
		cleanedGroupedAliases shouldEqual TestData.cleanedGroupedAliasesTestSet()
	}

	"Dead pages and only dead pages" should "be removed" in {
		val cleanedGroupedPages = LinkAnalysis.removeDeadPages(
			sc.parallelize(TestData.groupedPagesTestSet().toList),
			sc.parallelize(TestData.allPagesTestList()))
			.collect
			.toSet
		cleanedGroupedPages shouldEqual TestData.cleanedGroupedPagesTestSet()
	}
}
