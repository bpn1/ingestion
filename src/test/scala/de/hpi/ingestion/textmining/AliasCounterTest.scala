package de.hpi.ingestion.textmining

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.{SharedSparkContext, RDDComparisons}
import de.hpi.ingestion.implicits.CollectionImplicits._

class AliasCounterTest extends FlatSpec with RDDComparisons with SharedSparkContext with Matchers {
	"Counted aliases" should "have the same size as all links" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val allAliases = TestData.allAliasesTestRDD(sc)
		val countedAliases = AliasCounter
			.countAliases(articles)
		countedAliases.count shouldBe allAliases.count
	}

	they should "have the same aliases as all links" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val countedAliases = AliasCounter
				.countAliases(articles)
				.map(_.alias)
		assertRDDEquals(countedAliases, TestData.allAliasesTestRDD(sc))
	}

	they should "have counted any occurrence" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		AliasCounter
			.countAliases(articles)
			.collect
			.foreach(aliasCounter =>
				aliasCounter.totaloccurrences.get should be > 0)
	}

	they should "have consistent counts" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
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
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val countedAliases = AliasCounter
			.countAliases(articles)
		assertRDDEquals(countedAliases, TestData.countedAliasesTestRDD(sc))
	}

	"Extracted alias lists" should "contain all links and aliases" in {
		val aliasListsRDD = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.map(AliasCounter.extractAliasList)
		    .map(_.map(_.alias).toSet)
		val testAliasLists = TestData.aliasOccurrencesInArticlesTestRDD(sc)
			.map(data => data.links ++ data.noLinks)
		assertRDDEquals(aliasListsRDD, testAliasLists)
	}

	they should "have the alias counter set properly" in {
		sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.flatMap(AliasCounter.extractAliasList)
			.collect
			.foreach { alias =>
				alias.linkoccurrences.get should be <= 1
				alias.linkoccurrences.get should be >= 0
				alias.totaloccurrences.get shouldBe 1
			}
	}

	they should "be exactly the same Alias counters" in {
		val testAliasCounterData = TestData.startAliasCounterTestRDD(sc)
		val aliasCounterRDD = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.flatMap(AliasCounter.extractAliasList)
		assertRDDEquals(aliasCounterRDD, testAliasCounterData)
	}

	"Alias reduction function" should "properly add link occurrences" in {
		val testAliasCounterData = TestData.countedAliasesTestRDD(sc)
		val aliasCounterRDD = TestData.startAliasCounterTestRDD(sc)
			.map(alias => (alias.alias, alias))
			.reduceByKey(AliasCounter.aliasReduction)
		    .map(_._2)
		assertRDDEquals(aliasCounterRDD, testAliasCounterData)
	}

	"Alias counts" should "be exactly these tuples" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val mergedLinks = AliasCounter.run(List(articles).toAnyRDD(), sc)
			.fromAnyRDD[(String, Option[Int], Option[Int])]()
			.head
			.collect
			.toSet
		mergedLinks shouldEqual TestData.aliasCountsSet()
	}

	"Counted number of link occurrences" should "equal number of page references" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val expectedOccurrences = TestData.linksSet()
			.map(link => (link.alias, link.pages.foldLeft(0)(_ + _._2)))
			.toMap
		AliasCounter.run(List(articles).toAnyRDD(), sc)
			.fromAnyRDD[(String, Option[Int], Option[Int])]()
			.head
			.collect
			.foreach(link => link._2.get should be <= expectedOccurrences.getOrElse(link._1, 0))
	}
}
