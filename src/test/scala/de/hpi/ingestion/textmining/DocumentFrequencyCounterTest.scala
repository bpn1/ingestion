package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models.DocumentFrequency
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class DocumentFrequencyCounterTest extends FlatSpec with PrettyTester with SharedSparkContext with Matchers {
	"Document frequencies" should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val documentFrequencies = DocumentFrequencyCounter
			.countDocumentFrequencies(articles)
		documentFrequencies should not be empty
	}

	they should "be greater than zero" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		DocumentFrequencyCounter
			.countDocumentFrequencies(articles)
			.collect
			.foreach(df => df.count should be > 0)
	}

	they should "contain these document frequencies" in {
		val articles = sc.parallelize(TestData.parsedWikipediaTestSet().toList)
		val documentFrequencies = DocumentFrequencyCounter.countDocumentFrequencies(articles)
			.collect
			.toSet
		documentFrequencies should contain allElementsOf TestData.documentFrequenciesTestSet()
	}

	"Stemmed document frequencies" should "not contain unstemmed German words" in {
		val documentFrequencies = sc.parallelize(TestData.unstemmedDocumentFrequenciesTestSet().toList)
		val stemmedDF = DocumentFrequencyCounter
			.stemDocumentFrequencies(documentFrequencies)
			.map(_.word)
			.collect
			.toSet
		stemmedDF should contain noElementsOf TestData.unstemmedGermanWordsTestList()
	}

	they should "contain these document frequencies" in {
		val documentFrequencies = sc.parallelize(TestData.unstemmedDocumentFrequenciesTestSet().toList)
		val stemmedDF = DocumentFrequencyCounter
			.stemDocumentFrequencies(documentFrequencies)
			.collect
			.toSet
		stemmedDF should contain allElementsOf TestData.incorrectStemmedDocumentFrequenciesTestSet()
	}

	"Filtered document frequencies" should "not contain German stopwords" in {
		val documentFrequencies = sc.parallelize(TestData.documentFrequenciesTestSet().toList)
		val filteredWords = DocumentFrequencyCounter
			.filterDocumentFrequencies(documentFrequencies,
				1,
				TestData.germanStopwordsTestSet())
			.map(_.word)
			.collect
			.toSet
		filteredWords should contain noElementsOf TestData.germanStopwordsTestSet()
	}

	they should "not contain infrequent words" in {
		val threshold = DocumentFrequencyCounter.leastSignificantDocumentFrequency
		val documentFrequencies = sc.parallelize(TestData.documentFrequenciesTestSet().toList)
		val infrequentWords = DocumentFrequencyCounter
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
		val documentFrequencies = DocumentFrequencyCounter
			.countDocumentFrequencies(articles)
		val filteredDocumentFrequencies = DocumentFrequencyCounter
			.filterDocumentFrequencies(documentFrequencies,
				threshold,
				TestData.germanStopwordsTestSet())
			.collect
			.toSet
		val testFilteredDocumentFrequencies = TestData.filteredDocumentFrequenciesTestList()
			.toSet
		filteredDocumentFrequencies should contain allElementsOf testFilteredDocumentFrequencies
	}
}
