package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.textmining.models.{ParsedWikipediaEntry, Redirect}
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.rdd.RDD

class RedirectResolverTest extends FlatSpec with SharedSparkContext with Matchers {
	"All redirects in an entry" should "be resolved" in {
		val dict = TestData.smallRedirectMap()
		val links = TestData.linksWithRedirectsSet()
		val entry = ParsedWikipediaEntry("Test Entry 1", textlinks = links.toList)
		val resolvedLinks = RedirectResolver.resolveRedirects(entry, dict).textlinks.toSet
		val expectedLinks = TestData.linksWithResolvedRedirectsSet()
		resolvedLinks shouldBe expectedLinks
	}

	"Transitive redirects" should "be resolved" in {
		val redirectMap = TestData.transitiveRedirectMap()
		val cleanedMap = RedirectResolver.resolveTransitiveRedirects(redirectMap)
		val expectedMap = TestData.cleanedRedirectMap()
		cleanedMap shouldEqual expectedMap
	}

	"Entries" should "be cleaned of redirects" in {
		val redirectMap = TestData.cleanedRedirectMap()
		val entries = TestData.entriesWithRedirects()
			.map(RedirectResolver.resolveRedirects(_, redirectMap))
		val expectedEntries = TestData.entriesWithResolvedRedirects()
		entries shouldEqual expectedEntries
	}

	"Redirect dictionary" should "be built" in {
		val redirectEntries = sc.parallelize(TestData.parsedRedirectEntries().toList)
		val redirectDict = RedirectResolver.buildRedirectDict(redirectEntries)
		val expectedDict = TestData.redirectMap()
		redirectDict shouldEqual expectedDict
	}

	"Redirect resolver" should "resolve all redirects with self found redirects" in {
		val unresolvedEntries = sc.parallelize(TestData.parsedEntriesWithRedirects().toList)
		val emptyRedirectsMap = sc.parallelize(Map[String, String]().toList)
		val input = List(unresolvedEntries).toAnyRDD() ++ List(emptyRedirectsMap).toAnyRDD()
		val output = RedirectResolver.run(input, sc)
		val resolvedEntries = output.head
			.asInstanceOf[RDD[ParsedWikipediaEntry]]
			.collect
			.toSet
		val redirects = output(1)
			.asInstanceOf[RDD[Redirect]]
			.collect
			.toSet

		val expectedEntries = TestData.parsedEntriesWithResolvedRedirectsSet()
		val expectedRedirects = TestData.redirectMap().map(Redirect.tupled).toSet
		resolvedEntries shouldEqual expectedEntries
		redirects shouldEqual expectedRedirects
	}

	"Redirect resolver" should "resolve all redirects with given redirects" in {
		val unresolvedEntries = sc.parallelize(TestData.parsedEntriesWithRedirects().toList)
		val redirectsMap = sc.parallelize(TestData.redirectMap().toList.map(Redirect.tupled))
		val resolvedEntries = RedirectResolver.run(
			List(unresolvedEntries).toAnyRDD() ++ List(redirectsMap).toAnyRDD(),
			sc
		).fromAnyRDD[ParsedWikipediaEntry]()
			.head
			.collect
			.toSet

		val expectedEntries = TestData.parsedEntriesWithResolvedRedirectsSet()
		resolvedEntries shouldEqual expectedEntries
	}

}
