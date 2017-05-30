package de.hpi.ingestion.deduplication.similarity

import org.scalatest.{FlatSpec, Matchers}

class UrlCompareUnitTest extends FlatSpec with Matchers {
	"cleanUrlString" should "return the host anme of a given URL" in {
		val computed = UrlCompare.cleanUrlString("http://some.neckermann.berlin/some/stuff")
		val expected = "some.neckermann.berlin"
		computed shouldEqual expected
	}

	"UrlCompare" should "compare two hostnames with JaroWinkler" in {
		val computed1 = UrlCompare.compare("http://hpi.de", "https://www.hpi.de/callmefelix")
		val computed2 = UrlCompare.compare("http://augenbluten.de", "https://downtown.veitimwinkel.de")

		val expected1 = JaroWinkler.compare("hpi.de","hpi.de")
		val expected2 = JaroWinkler.compare("augenbluten.de","downtown.veitimwinkel.de")

		println(computed1 + " " + expected1)

		computed1 shouldEqual expected1
		computed2 shouldEqual expected2
	}
}
