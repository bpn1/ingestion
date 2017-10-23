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

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.textmining.TestData
import org.scalatest.{FlatSpec, Matchers}

class AliasCounterTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {
	"Counted aliases" should "have the same size as all links" in {
		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		val countedAliases = AliasCounter.countAliases(articles)
		countedAliases.count shouldBe TestData.allAliasesSet().size
	}

	they should "have the same aliases as all links" in {
		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		val countedAliases = AliasCounter
				.countAliases(articles)
				.map(_.alias)
				.collect
				.toSet
		countedAliases shouldEqual TestData.allAliasesSet()
	}

	they should "have counted any occurrence" in {
		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		AliasCounter
			.countAliases(articles)
			.collect
			.foreach(aliasCounter => aliasCounter.totaloccurrences.get should be > 0)
	}

	they should "have consistent counts" in {
		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		AliasCounter
			.countAliases(articles)
			.collect
			.foreach { aliasCounter =>
				aliasCounter.linkoccurrences.get should be <= aliasCounter.totaloccurrences.get
				aliasCounter.linkoccurrences.get should be >= 0
				aliasCounter.totaloccurrences.get should be >= 0
			}
	}

	they should "be exactly these counted aliases" in {
		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		val countedAliases = AliasCounter
			.countAliases(articles)
			.collect
			.toSet
		countedAliases shouldEqual TestData.countedAliasesSet()
	}

	"Extracted alias lists" should "contain all links and aliases" in {
		val aliasList = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
			.map(AliasCounter.extractAliasList)
			.map(_.map(_.alias).toSet)
			.collect
			.toList
		val testAliasLists = TestData.aliasOccurrencesInArticlesList()
			.map(data => data.links ++ data.noLinks)
		aliasList shouldEqual testAliasLists
	}

	they should "have the alias counter set properly" in {
		sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
			.flatMap(AliasCounter.extractAliasList)
			.collect
			.foreach { alias =>
				alias.linkoccurrences.get should be <= 1
				alias.linkoccurrences.get should be >= 0
				alias.totaloccurrences.get shouldBe 1
			}
	}

	they should "be exactly the same Alias counters" in {
		val aliasCounterRDD = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
			.flatMap(AliasCounter.extractAliasList)
		val expectedAliasCounterRDD = sc.parallelize(TestData.startAliasCounterList())
		assertRDDEquals(aliasCounterRDD, expectedAliasCounterRDD)
	}

	"Alias reduction function" should "properly add link occurrences" in {
		val aliasCounterSet = sc.parallelize(TestData.startAliasCounterList())
			.map(alias => (alias.alias, alias))
			.reduceByKey(AliasCounter.aliasReduction)
			.map(_._2)
			.collect
			.toSet
		aliasCounterSet shouldEqual TestData.countedAliasesSet()
	}

	"Alias counts" should "be exactly these tuples" in {
		val job = new AliasCounter
		job.parsedWikipedia = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		job.run(sc)
		val mergedLinks = job
			.aliasCounts
			.collect
			.toSet
		mergedLinks shouldEqual TestData.aliasCountsSet()
	}

	they should "count all links and found aliases" in {
		val job = new AliasCounter
		job.parsedWikipedia = sc.parallelize(TestData.aliasCounterArticles())
		job.run(sc)
		val aliasCounts = job
			.aliasCounts
			.collect
			.toSet
		val expectedCounts = TestData.multipleAliasCounts()
		aliasCounts shouldEqual expectedCounts
	}

	"Counted number of link occurrences" should "equal number of page references" in {
		val job = new AliasCounter
		job.parsedWikipedia = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		job.run(sc)
		val expectedOccurrences = TestData.linksSet()
			.map(link => (link.alias, link.pages.foldLeft(0)(_ + _._2)))
			.toMap
		job
			.aliasCounts
			.collect
			.foreach(link => link._2.get should be <= expectedOccurrences.getOrElse(link._1, 0))
	}
}
