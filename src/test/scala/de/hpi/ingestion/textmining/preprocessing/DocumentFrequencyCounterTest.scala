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
import de.hpi.ingestion.textmining.models.DocumentFrequency
import org.scalatest.{FlatSpec, Matchers}

class DocumentFrequencyCounterTest extends FlatSpec with SharedSparkContext with Matchers {
	"Document frequencies" should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		val documentFrequencies = DocumentFrequencyCounter
			.countDocumentFrequencies(articles)
		documentFrequencies should not be empty
	}

	they should "be greater than zero" in {
		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		DocumentFrequencyCounter
			.countDocumentFrequencies(articles)
			.collect
			.foreach(df => df.count should be > 0)
	}

	they should "contain these document frequencies" in {
		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		val documentFrequencies = DocumentFrequencyCounter.countDocumentFrequencies(articles)
			.collect
			.toSet
		documentFrequencies should contain allElementsOf TestData.documentFrequenciesSet()
	}

	they should "count stemmed words only once" in {
		val articles = sc.parallelize(TestData.unstemmedDFSet().toList)
		val documentFrequencies = DocumentFrequencyCounter
			.countDocumentFrequencies(articles)
			.collect
			.toSet
		documentFrequencies should contain allElementsOf TestData.stemmedDFSet()
	}

	"Filtered document frequencies" should "not contain German stopwords" in {
		val documentFrequencies = sc.parallelize(TestData.documentFrequenciesSet().toList)
		val filteredWords = DocumentFrequencyCounter
			.filterDocumentFrequencies(documentFrequencies, 1)
			.map(_.word)
			.collect
			.toSet
		filteredWords should contain noElementsOf TestData.germanStopwordsSet()
	}

	they should "not contain infrequent words" in {
		val threshold = DocumentFrequencyCounter.leastSignificantDocumentFrequency
		val documentFrequencies = sc.parallelize(TestData.documentFrequenciesSet().toList)
		val infrequentWords = DocumentFrequencyCounter
			.filterDocumentFrequencies(documentFrequencies, threshold)
			.filter(_.count < threshold)
			.collect
			.toSet
		infrequentWords shouldBe empty
	}

	they should "contain these document frequencies" in {
		val threshold = 3
		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		val documentFrequencies = DocumentFrequencyCounter
			.countDocumentFrequencies(articles)
		val filteredDocumentFrequencies = DocumentFrequencyCounter
			.filterDocumentFrequencies(documentFrequencies, threshold)
			.collect
			.toSet
		filteredDocumentFrequencies should contain allElementsOf TestData.filteredDocumentFrequenciesSet()
	}

	they should "be exactly these document frequencies" in {
		val oldThresh = DocumentFrequencyCounter.leastSignificantDocumentFrequency
		DocumentFrequencyCounter.leastSignificantDocumentFrequency = 3

		val job = new DocumentFrequencyCounter
		job.parsedWikipedia = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		job.run(sc)
		val df = job.documentFrequencies.collect.toList
		val expectedDf = TestData.filteredDocumentFrequenciesWithSymbols()
		df shouldEqual expectedDf

		DocumentFrequencyCounter.leastSignificantDocumentFrequency = oldThresh

		job.run(sc)
		val df2 = job.documentFrequencies.collect.toList
		df2 shouldBe empty
	}
}
