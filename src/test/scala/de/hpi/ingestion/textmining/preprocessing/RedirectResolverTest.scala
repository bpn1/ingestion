/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.textmining.preprocessing

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.textmining.TestData
import de.hpi.ingestion.textmining.models.{ParsedWikipediaEntry, Redirect}
import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, Matchers}

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
		val job = new RedirectResolver
		job.parsedWikipedia = sc.parallelize(TestData.parsedEntriesWithRedirects().toList)
		job.wikipediaRedirects = sc.parallelize(Map[String, String]().toList.map(Redirect.tupled))
		job.run(sc)
		val resolvedEntries = job.resolvedParsedWikipedia.collect.toSet
		val redirects = job.savedWikipediaRedirects.collect.toSet

		val expectedEntries = TestData.parsedEntriesWithResolvedRedirectsSet()
		val expectedRedirects = TestData.redirectMap().map(Redirect.tupled).toSet
		resolvedEntries shouldEqual expectedEntries
		redirects shouldEqual expectedRedirects
	}

	"Redirect resolver" should "resolve all redirects with given redirects" in {
		val job = new RedirectResolver
		job.parsedWikipedia = sc.parallelize(TestData.parsedEntriesWithRedirects().toList)
		job.wikipediaRedirects = sc.parallelize(TestData.redirectMap().toList.map(Redirect.tupled))
		job.run(sc)
		val resolvedEntries = job.resolvedParsedWikipedia.collect.toSet
		val expectedEntries = TestData.parsedEntriesWithResolvedRedirectsSet()
		resolvedEntries shouldEqual expectedEntries
	}
}
