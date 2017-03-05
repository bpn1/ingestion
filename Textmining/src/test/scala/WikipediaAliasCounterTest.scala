import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.{SharedSparkContext, RDDComparisons}

class WikipediaAliasCounterTest
	extends FlatSpec with RDDComparisons with SharedSparkContext with Matchers {
	"Counted aliases" should "have the same size as all links" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val allAliases = TestData.allAliasesTestRDD(sc)
		val countedAliases = WikipediaAliasCounter
			.countAliases(articles)
		countedAliases.count shouldBe allAliases.count
	}

	they should "have the same aliases as all links" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val countedAliases = WikipediaAliasCounter
				.countAliases(articles)
				.map(_.alias)
		assertRDDEquals(countedAliases, TestData.allAliasesTestRDD(sc))
	}

	they should "have counted any occurrence" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		WikipediaAliasCounter
			.countAliases(articles)
			.collect
			.foreach(aliasCounter =>
				aliasCounter.totaloccurrences should be > 0)
	}

	they should "have consistent counts" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		WikipediaAliasCounter
			.countAliases(articles)
			.collect
			.foreach { aliasCounter =>
				aliasCounter.linkoccurrences should be <= aliasCounter.totaloccurrences
				aliasCounter.linkoccurrences should be >= 0
				aliasCounter.totaloccurrences should be >= 0
			}
	}

	they should "be exactly these counted aliases" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val countedAliases = WikipediaAliasCounter
			.countAliases(articles)
		assertRDDEquals(countedAliases, TestData.countedAliasesTestRDD(sc))
	}

	"Extracted alias lists" should "contain all links and aliases" in {
		val aliasListsRDD = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.map(WikipediaAliasCounter.extractAliasList)
		    .map(_.map(_.alias).toSet)
		val testAliasLists = TestData.aliasOccurrencesInArticlesTestRDD(sc)
		    .map(data => data.links ++ data.noLinks)
		assertRDDEquals(aliasListsRDD, testAliasLists)
	}

	they should "have the alias counter set properly" in {
		sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.flatMap(WikipediaAliasCounter.extractAliasList)
			.collect
		    .foreach { alias =>
				alias.linkoccurrences should be <= 1
				alias.linkoccurrences should be >= 0
				alias.totaloccurrences shouldBe 1
			}

	}

	they should "be exactly the same Alias counters" in {
		val testAliasCounterData = TestData.startAliasCounterTestRDD(sc)
		val aliasCounterRDD = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.flatMap(WikipediaAliasCounter.extractAliasList)
		assertRDDEquals(aliasCounterRDD, testAliasCounterData)
	}

	"Alias reduction function" should "properly add link occurences" in {
		val testAliasCounterData = TestData.countedAliasesTestRDD(sc)
		val aliasCounterRDD = TestData.startAliasCounterTestRDD(sc)
			.map(alias => (alias.alias, alias))
			.reduceByKey(WikipediaAliasCounter.aliasReduction(_, _))
		    .map(_._2)
		assertRDDEquals(aliasCounterRDD, testAliasCounterData)
	}

	"Probability that word is link" should "be calculated correctly" in {
		val linkProbabilities = TestData.countedAliasesTestRDD(sc)
			.map(countedAlias =>
				(countedAlias.alias, WikipediaAliasCounter.probabilityIsLink(countedAlias)))
		assertRDDEquals(linkProbabilities, TestData.linkProbabilitiesTestRDD(sc))
	}

}
