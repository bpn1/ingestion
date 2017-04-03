package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models.DocumentFrequency
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class WikipediaContextExtractorTest
	extends FlatSpec with PrettyTester with SharedSparkContext with Matchers {
	"Link contexts" should "contain all occurring page names of links and only once" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val pageNames = WikipediaContextExtractor
			.extractAllContexts(articles)
			.map(_.pagename)
			.sortBy(identity)
		assert(areRDDsEqual(pageNames, TestData.allPageNamesTestRDD(sc)))
	}

	they should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val contexts = WikipediaContextExtractor
			.extractAllContexts(articles)
		contexts should not be empty
	}

	they should "contain any words" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		WikipediaContextExtractor
			.extractAllContexts(articles)
			.collect
			.foreach(context => context.words should not be empty)
	}

	"Link contexts of one article" should "contain all occurring page names of links" in {
		val articleTitle = "Testartikel"
		val article = TestData.getArticle(articleTitle)
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
		val article = TestData.getArticle(articleTitle)
		WikipediaContextExtractor
			.extractLinkContextsFromArticle(article, new CleanCoreNLPTokenizer)
			.foreach(context => context.words should contain allElementsOf testWords)
	}

	"Word set from one article" should "be exactly this word set" in {
		val articleTitle = "Testartikel"
		val testWords = TestData.articleContextwordSets()(articleTitle)
		val article = TestData.getArticle(articleTitle)
		val wordSet = WikipediaContextExtractor.textToWordSet(
			article.getText(),
			new CleanCoreNLPTokenizer)
		wordSet shouldEqual testWords
	}

	"Document frequencies" should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val documentFrequencies = WikipediaContextExtractor
			.countDocumentFrequencies(articles)
		documentFrequencies should not be empty
	}

	they should "be greater than zero" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		WikipediaContextExtractor
			.countDocumentFrequencies(articles)
			.collect
			.foreach(df => df.count should be > 0)
	}

	they should "contain these document frequencies" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val documentFrequencies = WikipediaContextExtractor.countDocumentFrequencies(articles)
			.collect
			.toSet
		documentFrequencies should contain allElementsOf TestData.documentFrequenciesTestSet()
	}

	"Stemmed document frequencies" should "not contain unstemmed German words" in {
		val documentFrequencies = sc.parallelize(TestData.unstemmedDFTestSet().toList)
		val stemmedDF = WikipediaContextExtractor
			.stemDocumentFrequencies(documentFrequencies)
			.map(_.word)
			.collect
			.toSet
		stemmedDF should contain noElementsOf TestData.unstemmedGermanWordsTestList()
	}

	they should "contain these document frequencies" in {
		val documentFrequencies = sc.parallelize(TestData.unstemmedDFTestSet().toList)
		val stemmedDF = WikipediaContextExtractor
			.stemDocumentFrequencies(documentFrequencies)
			.collect
			.toSet
		stemmedDF should contain allElementsOf TestData.stemmedDFTestSet()
	}

	"Filtered document frequencies" should "not contain German stopwords" in {
		val documentFrequencies = sc.parallelize(TestData.documentFrequenciesTestSet().toList)
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
		val documentFrequencies = sc.parallelize(TestData.documentFrequenciesTestSet().toList)
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
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val documentFrequencies = WikipediaContextExtractor
			.countDocumentFrequencies(articles)
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

	"Requested document frequency" should "be correct for known words or else (threshold - 1)" in {
		val threshold = 3
		val requestedDocumentFrequencies = TestData.documentFrequenciesTestSet()
			.map { documentFrequency =>
				val count = WikipediaContextExtractor.getDocumentFrequency(
					documentFrequency.word,
					sc.parallelize(TestData.filteredDocumentFrequenciesTestList()),
					threshold)
				DocumentFrequency(documentFrequency.word, count)
			}
		requestedDocumentFrequencies shouldEqual TestData.requestedDocumentFrequenciesTestSet()
	}
}
