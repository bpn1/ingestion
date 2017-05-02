package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.{Alias, Page}
import org.apache.spark.rdd.RDD

class LinkAnalysisTest extends FlatSpec with SharedSparkContext with Matchers {
	"Valid links" should "be extracted" in {
		val articles = sc.parallelize(TestData.closedParsedWikipediaSet().toList)
		val validLinks = LinkAnalysis
			.extractValidLinks(articles)
			.collect
		validLinks should not be empty
	}

	they should "be exactly these links" in {
		val articles = sc.parallelize(TestData.closedParsedWikipediaSet().toList)
		val validLinks = LinkAnalysis
			.extractValidLinks(articles)
			.collect
			.toSet
		validLinks shouldEqual TestData.validLinkSet()
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
			.collect
			.toSet
		groupedAliases shouldEqual TestData.groupedAliasesSet()
	}

	"Aliases grouped by page names" should "not be empty" in {
		val links = sc.parallelize(TestData.validLinkSet().toList)
		val groupedPageNames = LinkAnalysis
			.groupByPageNames(links)
			.collect
			.toSet
		groupedPageNames should not be empty
	}

	they should "not have empty page names" in {
		val links = sc.parallelize(TestData.validLinkSet().toList)
		LinkAnalysis
			.groupByPageNames(links)
			.collect
			.foreach(page => page.page should not be empty)
	}

	they should "contain at least one alias per page name" in {
		val links = sc.parallelize(TestData.validLinkSet().toList)
		LinkAnalysis
			.groupByPageNames(links)
			.collect
			.foreach(pages => pages.aliases should not be empty)
	}

	they should "not contain empty aliases" in {
		val links = sc.parallelize(TestData.validLinkSet().toList)
		LinkAnalysis
			.groupByPageNames(links)
			.flatMap(_.aliases)
			.map(_._1)
			.collect
			.foreach(alias => alias should not be "")
	}

	they should "be exactly these page names" in {
		val links = sc.parallelize(TestData.validLinkSet().toList)
		val groupedPages = LinkAnalysis
			.groupByPageNames(links)
			.collect
			.toSet
		groupedPages shouldEqual TestData.groupedPagesSet()
	}

	"Link data" should "be grouped and counted" in {
		val linkData = TestData.validLinkSet()
			.map(link => (link.alias, List(link.page)))
		val groupedData = LinkAnalysis.groupLinks(sc.parallelize(linkData.toList)).collect.toSet
		val expectedData = TestData.groupedAliasesSet().map(alias => (alias.alias, alias.pages))
		groupedData shouldEqual expectedData
	}

	"Grouped aliases and page names from incomplete article set" should "be empty" in {
		val articles = sc.parallelize(TestData.smallerParsedWikipediaList())
		val List(groupedAliases, groupedPages) = LinkAnalysis.run(List(articles).toAnyRDD(), sc)
		val groupedAliasesSet = groupedAliases.asInstanceOf[RDD[Alias]]
			.collect
		val groupedPagesSet = groupedPages.asInstanceOf[RDD[Page]]
			.collect
		groupedAliasesSet shouldBe empty
		groupedPagesSet shouldBe empty
	}
}
