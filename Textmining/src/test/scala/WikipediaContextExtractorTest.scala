import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FlatSpec
import org.scalatest._

class WikipediaContextExtractorTest
	extends FlatSpec with PrettyTester with SharedSparkContext with Matchers {
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
		contexts should not be empty
	}

	they should "contain any words" in {
		WikipediaContextExtractor
			.extractAllContexts(TestData.parsedWikipediaTestRDD(sc))
			.collect
			.foreach(context => context.words should not be empty)
	}

	"Link contexts of one article" should "contain all occurring page names of links" in {
		val articleTitle = "Testartikel"
		val article = TestData.getArticle(articleTitle, sc)
		val pageNames =
			WikipediaContextExtractor.extractLinkContextsFromArticle(
				article,
				new CleanCoreNLPTokenizer)
				.map(_.pagename)
		pageNames shouldEqual TestData.allPageNamesOfTestArticleList()
	}

	they should "contain at least these words" in {
		val articleTitle = "Streitberg (Brachttal)"
		val testWords = TestData.articleContextwordSets()(articleTitle)
		val article = TestData.getArticle(articleTitle, sc)
		WikipediaContextExtractor
			.extractLinkContextsFromArticle(article, new CleanCoreNLPTokenizer)
			.foreach(context => context.words should contain allElementsOf testWords)
	}

	"Document frequencies" should "not be empty" in {
		val documentFrequencies = WikipediaContextExtractor
			.countDocumentFrequencies(TestData.parsedWikipediaTestRDD(sc))
		documentFrequencies should not be empty
	}

	they should "be greater than zero" in {
		WikipediaContextExtractor
			.countDocumentFrequencies(TestData.parsedWikipediaTestRDD(sc))
			.collect
			.foreach(df => df.count should be > 0)
	}

	they should "contain these document frequencies" in {
		val documentFrequencies = WikipediaContextExtractor
			.countDocumentFrequencies(TestData.parsedWikipediaTestRDD(sc))
			.collect
			.toSet
		val testDocumentFrequencies = TestData.documentFrequenciesTestList()
			.toSet
		documentFrequencies should contain allElementsOf testDocumentFrequencies
	}

	"Filtered document frequencies" should "not contain German stopwords" in {
		val documentFrequencies = sc.parallelize(TestData.documentFrequenciesTestList())
		val filteredWords = WikipediaContextExtractor
			.filterDocumentFrequencies(documentFrequencies,
				1,
				TestData.germanStopwordsTestSet())
			.map(_.word)
			.collect
			.toSet
		filteredWords should contain noElementsOf TestData.germanStopwordsTestSet()
	}

	they should "not contain infrequent words" in {
		val threshold = WikipediaContextExtractor.leastSignificantDocumentFrequency
		val documentFrequencies = sc.parallelize(TestData.documentFrequenciesTestList())
		val infrequentWords = WikipediaContextExtractor
			.filterDocumentFrequencies(documentFrequencies,
				threshold,
				Set[String]())
			.filter(_.count < threshold)
			.collect
			.toSet
		infrequentWords shouldBe empty
	}

	they should "contain these document frequencies" in {
		val threshold = 3
		val documentFrequencies = WikipediaContextExtractor
			.countDocumentFrequencies(TestData.parsedWikipediaTestRDD(sc))
		val filteredDocumentFrequencies = WikipediaContextExtractor
			.filterDocumentFrequencies(documentFrequencies,
				threshold,
				TestData.germanStopwordsTestSet())
			.collect
			.toSet
		val testFilteredDocumentFrequencies = TestData.filteredDocumentFrequenciesTestList()
			.toSet
		filteredDocumentFrequencies should contain allElementsOf testFilteredDocumentFrequencies
	}

	"Word set from one article" should "be exactly this word set" in {
		val articleTitle = "Testartikel"
		val testWords = TestData.articleContextwordSets()(articleTitle)
		val article = TestData.getArticle(articleTitle, sc)
		val wordSet = WikipediaContextExtractor.textToWordSet(
			article.getText(),
			new CleanCoreNLPTokenizer)
		wordSet shouldEqual testWords
	}

	"Requested document frequency" should "be correct for known words or else (threshold - 1)" in {
		val threshold = 3
		val requestedDocumentFrequencies = TestData.documentFrequenciesTestList()
			.map(documentFrequency =>
				WikipediaContextExtractor.getDocumentFrequency(
					documentFrequency.word,
					sc.parallelize(TestData.filteredDocumentFrequenciesTestList()),
					threshold))
		requestedDocumentFrequencies shouldEqual TestData.requestedDocumentFrequenciesTestList()
	}
}
