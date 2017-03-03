import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import WikiClasses._

class WikipediaAliasCounterTest extends FlatSpec with PrettyTester with SharedSparkContext {
	"Counted aliases" should "have the same size as all links" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val allAliases = TestData.allAliasesTestRDD(sc)
		val countedAliases =
			WikipediaAliasCounter.countAllAliasOccurrences(articles)
		assert(countedAliases.count() == allAliases.count)
	}

	they should "have the same aliases as all links" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val countedAliases =
			WikipediaAliasCounter.countAllAliasOccurrences(articles)
				.map(_.alias)
				.sortBy(identity)
		assert(areRDDsEqual(countedAliases, TestData.allAliasesTestRDD(sc)))
	}

	they should "have counted any occurrence" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		WikipediaAliasCounter.countAllAliasOccurrences(articles)
			.collect
			.foreach(aliasCounter => assert(aliasCounter.totaloccurrences > 0))
	}

	they should "have consistent counts" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		WikipediaAliasCounter.countAllAliasOccurrences(articles)
			.collect
			.foreach { aliasCounter =>
				assert(aliasCounter.linkoccurrences <= aliasCounter.totaloccurrences)
				assert(aliasCounter.linkoccurrences >= 0 && aliasCounter.totaloccurrences >= 0)
			}
	}

	they should "be exactly these counted aliases" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val countedAliases = WikipediaAliasCounter
			.countAllAliasOccurrences(articles)
			.sortBy(_.alias)
		assert(areRDDsEqual(countedAliases, TestData.countedAliasesTestRDD(sc)))
	}

	"Alias occurrences" should "be correct identified as link or no link" in {
		val testOccurences = TestData.aliasOccurrencesInArticlesTestRDD(sc)
		val aliasOccurrencesInArticles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
			.map(article => WikipediaAliasCounter.identifyAliasOccurrencesInArticle(article))
		assert(areRDDsEqual(aliasOccurrencesInArticles, testOccurences))
	}

	"Identified aliases" should "not be link and no link in the same article" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		articles
			.map(article => WikipediaAliasCounter.identifyAliasOccurrencesInArticle(article))
			.collect
			.foreach(occurrences =>
				assert(occurrences.links.intersect(occurrences.noLinks).isEmpty))
	}

	"Probability that word is link" should "be calculated correctly" in {
		val linkProbabilities = TestData.countedAliasesTestRDD(sc)
			.map(countedAlias =>
				(countedAlias.alias, WikipediaAliasCounter.probabilityIsLink(countedAlias)))
		assert(areRDDsEqual(linkProbabilities, TestData.linkProbabilitiesTestRDD(sc)))
	}

}
