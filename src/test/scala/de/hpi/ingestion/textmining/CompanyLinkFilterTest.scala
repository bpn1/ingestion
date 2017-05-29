package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.ParsedWikipediaEntry

class CompanyLinkFilterTest extends FlatSpec with Matchers with SharedSparkContext with RDDComparisons {

	"Company pages" should "be extracted" in {
		val wikidataEntities = sc.parallelize(TestData.wikidataEntities())
		val companyPages = CompanyLinkFilter.extractCompanyPages(wikidataEntities)
		val expectedPages = sc.parallelize(TestData.wikidataCompanyPages())
		assertRDDEquals(companyPages, expectedPages)
	}

	"Company aliases" should "be extracted" in {
		val pages = sc.parallelize(TestData.companyPages())
		val companyPages = sc.parallelize(TestData.wikidataCompanyPages())
		val companyAliases = CompanyLinkFilter.extractCompanyAliases(pages, companyPages)
		val expectedAliases = sc.parallelize(TestData.companyAliases().toList)
		assertRDDEquals(companyAliases, expectedAliases)
	}

	"Wikipedia links" should "be filtered" in {
		val companyAliases = TestData.companyAliases()
		val cleanedArticles = TestData.unfilteredCompanyLinksEntries()
			.map(CompanyLinkFilter.filterCompanyLinks(_, companyAliases))
		val expectedArticles = TestData.filteredCompanyLinksEntries()
		cleanedArticles shouldEqual expectedArticles
	}

	"Parsed Wikipedia Entries" should "be cleaned" in {
		val wikidataEntities = List(sc.parallelize(TestData.wikidataEntities())).toAnyRDD()
		val pages = List(sc.parallelize(TestData.companyPages())).toAnyRDD()
		val articles = List(sc.parallelize(TestData.unfilteredCompanyLinksEntries())).toAnyRDD()
		val input = wikidataEntities ++ pages ++ articles
		val cleanedArticles = CompanyLinkFilter.run(input, sc).fromAnyRDD[ParsedWikipediaEntry]().head
		val expectedArticles = sc.parallelize(TestData.filteredCompanyLinksEntries())
		assertRDDEquals(cleanedArticles, expectedArticles)
	}
}
