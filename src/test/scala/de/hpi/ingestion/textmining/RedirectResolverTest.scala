package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.textmining.models.ParsedWikipediaEntry
import org.scalatest.{FlatSpec, Matchers}


class RedirectResolverTest extends FlatSpec with SharedSparkContext with Matchers {
	"All redirects in an entry" should "be resolved" in {
		val dict = TestData.testRedirectDict()
		val links = TestData.testLinksWithRedirects()
		val entry = ParsedWikipediaEntry("Test Entry 1", textlinks = links.toList)
		val resolvedLinks = RedirectResolver.resolveRedirects(entry, dict).textlinks.toSet
		val expectedLinks = TestData.testLinksWithResolvedRedirects()
		resolvedLinks shouldBe expectedLinks
	}

	"Transitive redirects" should "be resolved" in {
		val redirectMap = TestData.redirectMap()
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

}
