package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.DocumentFrequency

class DocumentFrequencyCounterTest extends FlatSpec with SharedSparkContext with Matchers {
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

	they should "count stemmed words only once" in {
		val articles = sc.parallelize(TestData.unstemmedDFTestSet().toList)
		val documentFrequencies = DocumentFrequencyCounter
			.countDocumentFrequencies(articles)
			.collect
			.toSet
		documentFrequencies should contain allElementsOf TestData.stemmedDFTestSet()
	}

	"Filtered document frequencies" should "not contain German stopwordsSet" in {
		val documentFrequencies = sc.parallelize(TestData.documentFrequenciesTestSet().toList)
		val filteredWords = DocumentFrequencyCounter
			.filterDocumentFrequencies(documentFrequencies, 1)
			.map(_.word)
			.collect
			.toSet
		filteredWords should contain noElementsOf TestData.germanStopwordsTestSet()
	}

	they should "not contain infrequent words" in {
		val threshold = DocumentFrequencyCounter.leastSignificantDocumentFrequency
		val documentFrequencies = sc.parallelize(TestData.documentFrequenciesTestSet().toList)
		val infrequentWords = DocumentFrequencyCounter
			.filterDocumentFrequencies(documentFrequencies, threshold)
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
			.filterDocumentFrequencies(documentFrequencies, threshold)
			.collect
			.toSet
		val testFilteredDocumentFrequencies = TestData.filteredDocumentFrequenciesTestList()
			.toSet
		filteredDocumentFrequencies should contain allElementsOf testFilteredDocumentFrequencies
	}

	they should "be exactly these document frequencies" in {
		val oldThresh = DocumentFrequencyCounter.leastSignificantDocumentFrequency

		DocumentFrequencyCounter.leastSignificantDocumentFrequency = 3
		val input = List(sc.parallelize(TestData.parsedWikipediaTestSet().toList)).toAnyRDD()
		val df = DocumentFrequencyCounter.run(input, sc).fromAnyRDD[DocumentFrequency]().head.collect.toList
		val expectedDf = TestData.filteredDocumentFrequenciesWithSymbols()
		df shouldEqual expectedDf

		DocumentFrequencyCounter.leastSignificantDocumentFrequency = oldThresh

		val df2 = DocumentFrequencyCounter.run(input, sc).fromAnyRDD[DocumentFrequency]().head.collect.toList
		df2 shouldBe empty
	}
}
