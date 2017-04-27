package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class LinkAnalysisTest extends FlatSpec with SharedSparkContext with Matchers {
	"Valid links" should "be extracted" in {
		val articles = sc.parallelize(TestData.closedParsedWikipediaSet().toList)
		val validLinks = LinkAnalysis
			.extractValidLinks(articles)
			.collect
		validLinks should not be empty
	}

	"Page names grouped by aliases" should "not be empty" in {
		val links = sc.parallelize(TestData.validLinkSet().toList)
		val groupedAliases = LinkAnalysis
			.groupByAliases(links)
			.collect
			.toSet
		groupedAliases should not be empty
	}

	they should "not have empty aliases" in {
		val links = sc.parallelize(TestData.validLinkSet().toList)
		LinkAnalysis
			.groupByAliases(links)
			.collect
			.foreach(alias => alias.alias should not be empty)
	}

	they should "contain at least one page name per alias" in {
		val links = sc.parallelize(TestData.validLinkSet().toList)
		LinkAnalysis
			.groupByAliases(links)
			.collect
			.foreach(alias => alias.pages should not be empty)
	}

	they should "not contain empty page names" in {
		val links = sc.parallelize(TestData.validLinkSet().toList)
		LinkAnalysis
			.groupByAliases(links)
			.flatMap(_.pages)
			.map(_._1)
			.collect
			.foreach(page => page should not be "")
	}

	they should "be exactly these aliases" in {
		val links = sc.parallelize(TestData.validLinkSet().toList)
		val groupedAliases = LinkAnalysis
			.groupByAliases(links)
			.map(alias => (alias.alias, alias.pages))
			.collect
			.toSet
		val groupedAliasesTest = TestData.groupedAliasesTestSet()
			.map(alias => (alias.alias, alias.pages))
		groupedAliases shouldEqual groupedAliasesTest
	}

	"Aliases grouped by page names" should "not be empty" in {
		val groupedPageNames = LinkAnalysis
			.groupByPageNames(sc.parallelize(TestData.smallerParsedWikipediaList()))
			.collect
			.toSet
		groupedPageNames should not be empty
	}

	they should "not have empty page names" in {
		LinkAnalysis
			.groupByPageNames(sc.parallelize(TestData.smallerParsedWikipediaList()))
			.collect
			.foreach(page => page.page should not be empty)
	}

	they should "contain at least one alias per page name" in {
		LinkAnalysis
			.groupByPageNames(sc.parallelize(TestData.smallerParsedWikipediaList()))
			.collect
			.foreach(pages => pages.aliases should not be empty)
	}

	they should "not contain empty aliases" in {
		LinkAnalysis
			.groupByPageNames(sc.parallelize(TestData.smallerParsedWikipediaList()))
			.flatMap(_.aliases)
			.map(_._1)
			.collect
			.foreach(alias => alias should not be "")
	}

	they should "be exactly these page names" in {
		val groupedPages = LinkAnalysis
			.groupByPageNames(sc.parallelize(TestData.smallerParsedWikipediaList()))
			.collect
			.toSet
		groupedPages shouldEqual TestData.groupedPagesTestSet()
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

	"Grouped aliases and page names from incomplete article set" should "be empty" in {
		val articles = sc.parallelize(TestData.smallerParsedWikipediaList())
		val (groupedAliases, groupedPages) = LinkAnalysis.run(articles)
		val groupedAliasesSet = groupedAliases
			.collect
		val groupedPagesSet = groupedPages
			.collect
		groupedAliasesSet shouldBe empty
		groupedPagesSet shouldBe empty
	}
}
