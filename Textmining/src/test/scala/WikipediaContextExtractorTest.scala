import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec
import WikiClasses._

class WikipediaContextExtractorTest extends FlatSpec with PrettyTester with SharedSparkContext {
	"Link contexts" should "contain all occurring page names of links and only once" in {
		val pageNames = WikipediaContextExtractor
			.extractAllContexts(TestData.parsedWikipediaTestRDD(sc))
			.map(_.pagename)
			.sortBy(identity)
		assert(areRDDsEqual(pageNames, TestData.allPageNamesTestRDD(sc)))
	}

	they should "not be empty" in {
		val contexts = WikipediaContextExtractor
			.extractAllContexts(TestData.parsedWikipediaTestRDD(sc))
		assert(!contexts.isEmpty)
	}

	they should "contain any words" in {
		WikipediaContextExtractor
			.extractAllContexts(TestData.parsedWikipediaTestRDD(sc))
			.collect
			.foreach(context => assert(context.words.nonEmpty))
	}

	"Link contexts of one article" should "contain all occurring page names of links" in {
		val articleTitle = "Testartikel"
		val article = TestData.getArticle(articleTitle, sc)
		val pageNames =
			WikipediaContextExtractor.extractLinkContextsFromArticle(
				article,
				new CleanCoreNLPTokenizer)
			.map(_.pagename)
		assert(pageNames == TestData.allPageNamesOfTestArticleList())
	}

	they should "contain at least these words" in {
		val articleTitle = "Streitberg (Brachttal)"
		val testWords = TestData.articleContextwordSets()(articleTitle)
		val article = TestData.getArticle(articleTitle, sc)
		WikipediaContextExtractor.extractLinkContextsFromArticle(article, new CleanCoreNLPTokenizer)
			.foreach(context => assert(isSubset(testWords, context.words)))
	}

	"Document frequencies" should "not be empty" in {
		val documentFrequencies = WikipediaContextExtractor
			.countDocumentFrequencies(TestData.parsedWikipediaTestRDD(sc))
		assert(!documentFrequencies.isEmpty)
	}

	they should "be greater than zero" in {
		WikipediaContextExtractor
			.countDocumentFrequencies(TestData.parsedWikipediaTestRDD(sc))
			.collect
			.foreach(df => assert(df.count > 0))
	}

	they should "contain these document frequencies" in {
		val documentFrequencies = WikipediaContextExtractor
			.countDocumentFrequencies(TestData.parsedWikipediaTestRDD(sc))
			.collect
			.toSet
		val testDocumentFrequencies = TestData.documentFrequenciesTestRDD(sc)
			.collect
			.toSet
		assert(isSubset(testDocumentFrequencies, documentFrequencies))
	}

	"Word set from one article" should "be exactly this word set" in {
		val articleTitle = "Testartikel"
		val testWords = TestData.articleContextwordSets()(articleTitle)
		val article = TestData.getArticle(articleTitle, sc)
		val wordSet = WikipediaContextExtractor.textToWordSet(
			article.getText(),
			new CleanCoreNLPTokenizer)
		assert(areSetsEqual(wordSet, testWords))
	}
}
