package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import scala.collection.mutable

class RedirectResolverTest extends FlatSpec with SharedSparkContext with Matchers {
	"All redirects in an entry" should "be resolved" in {
		val dict = mutable.Map[String, String]() ++ TestData.testRedirectDict
		val links= TestData.testLinksWithRedirects
		val resolvedLinks = links.map(
			RedirectResolver.cleanLinkOfRedirects(_, dict)
		)
		resolvedLinks shouldBe TestData.testLinksWithResolvedRedirects
	}

	"All looped or bad redirect pages" should "be filtered" in {
		val entries = TestData.testRedirectCornerCaseEntries
		entries.filter { case (entry) =>
			RedirectResolver.isValidRedirect(entry.textlinks.head, entry.title)
		} shouldBe empty
	}
}
