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

package de.hpi.ingestion.sentenceembedding

import breeze.linalg.DenseVector
import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.sentenceembedding.mock.MockSentenceEmbeddings
import de.hpi.ingestion.textmining.{TestData => TextTestData}
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits._

import scala.collection.mutable
import scala.math.BigDecimal

class SentenceEmbeddingsTest extends FlatSpec with Matchers with SharedSparkContext {
    "Weight parameter" should "be set if provided" in {
        var job = new MockSentenceEmbeddings
        val defaultWeightParam = job.weightParam
        job.execute(sc, Array("-Fweightparam=1e-5"))
        job.weightParam shouldEqual defaultWeightParam
        job.setWeightParam()
        job.weightParam shouldEqual 1e-5
        job = new MockSentenceEmbeddings
        job.execute(sc, Array("-Fweightparam=1e-5asd"))
        job.setWeightParam()
        job.weightParam shouldEqual defaultWeightParam
        job = new MockSentenceEmbeddings
        job.execute(sc, Array("-Fweightparam=0.2134"))
        job.setWeightParam()
        job.weightParam shouldEqual 0.2134
    }

    "Word frequencies" should "be parsed" in {
        val wordFrequencyFile = TestData.wordFrequencies()
        val wordFrequencies = wordFrequencyFile.flatMap(SentenceEmbeddings.convertLineToWordFrequency)
        val expectedWordFrequencies = TestData.parsedWordFrequencies
        wordFrequencies shouldEqual expectedWordFrequencies
    }

    "Word weights" should "be calculated" in {
        val job = new SentenceEmbeddings
        job.weightParam = 1.0
        val wordFrequencies = sc.parallelize(TestData.parsedWordFrequencies)
        val totalWordFrequency = TestData.totalWordFrequency
        val wordWeights = job.computeWordWeights(wordFrequencies, totalWordFrequency)
        val expectedWordWeights = TestData.wordWeights
        wordWeights shouldEqual expectedWordWeights
    }

    "Sentences" should "be preprocessed" in {
        val tokenizedSentences = TestData.tokenizedSentences
        val stopwords = TestData.stopwords()
        val preprocessedSentences = tokenizedSentences
            .map(SentenceEmbeddings.preprocessSentences(_, stopwords, removeStopwords = true, removeDigits = true))
        val expectedSentences = TestData.preprocessedSentences
        preprocessedSentences shouldEqual expectedSentences
    }

    they should "preprocessed and filtered" in {
        val sentences = sc.parallelize(TestData.tokenizedSentences).zipWithIndex().map(_.swap)
        val stopwords = TestData.stopwords()
        val preprocessedSentences = SentenceEmbeddings.preprocessSentences(sentences, stopwords)
            .collect
            .toSet
        val expectedSentences = TestData.indexedPreprocessedSentences
        preprocessedSentences shouldEqual expectedSentences
    }

    they should "be converted to sentence embeddings" in {
        val wordEmbeddings = TestData.exampleEmbeddings
        val weights = TestData.exampleWeights
        val sentences = TestData.exampleSentences
        val sentenceEmbeddings = sentences
            .flatMap(SentenceEmbeddings.convertSentenceToSentenceEmbedding(_, wordEmbeddings, weights))
        val expectedEmbeddings = TestData.exampleSentenceEmbeddingTuples.map(_._2)
        sentenceEmbeddings should have length expectedEmbeddings.length
        sentenceEmbeddings.zip(expectedEmbeddings).foreach { case (vector, exVector) =>
            vector.data.toList.zip(exVector.data.toList).foreach { case (value, exValue) =>
                value === exValue +- 0.000000000000001 shouldBe true
            }
        }
    }

    "Tokens" should "be cleaned" in {
        val tokens = TestData.dirtyTokens
        List((false, false), (false, true), (true, false), (true, true)).map { case (digits, hashtags) =>
            val cleanedTokens = SentenceEmbeddings.preprocessSentences(
                tokens,
                removeDigits = digits,
                removeHashtags = hashtags)
            cleanedTokens shouldEqual TestData.cleanTokens(digits, hashtags)
        }
    }

    "Word embeddings" should "be parsed" in {
        val embeddings = TestData.wordEmbeddings()
        val parsedEmbeddings = embeddings
            .flatMap(SentenceEmbeddings.convertLineToEmbedding)
            .map { case (word, embedding) => (word, embedding.toList) }
        val expectedEmbeddings = TestData.parsedEmbeddings
        parsedEmbeddings.foreach { case (word, embedding) => embedding should have length 300 }
        parsedEmbeddings shouldEqual expectedEmbeddings
    }

    they should "be added to a mutable collection" in {
        val embedddings = TestData.wordEmbeddings()
        val embeddingsMap = mutable.Map.empty[String, Array[Double]]
        embedddings.foreach(SentenceEmbeddings.addWordEmbeddingFromFile(_, embeddingsMap))
        val expectedEmbeddings = TestData.parsedEmbeddings.toMap
        embeddingsMap.toMap.mapValues(_.toList) shouldEqual expectedEmbeddings
    }

    "Principal component" should "be removed from sentence embeddings" in {
        val embeddings = sc.parallelize(TestData.exampleSentenceEmbeddingTuples)
        val cleanedEmbeddings = SentenceEmbeddings.removePrincipalComponent(embeddings).collect.toList
        cleanedEmbeddings should have length 3
        cleanedEmbeddings.foreach { case (index, vector) => vector should have length 4 }
    }

    "Sentence embeddings" should "be computed from the sentences" in {
        val job = new SentenceEmbeddings
        job.embeddingStreamFunction = TextTestData.docfreqStream("wordembeddings_example", "/sentenceembeddings/")
        val sentences = sc.parallelize(TestData.exampleSentences).zipWithIndex().map(_.swap)
        val weights = TestData.exampleWeights
        val sentenceEmbeddings = job.computeSentenceEmbeddings(sentences, weights)
            .map { case (index, vector) =>
                val roundedData = vector.data
                    .toList
                    .map(BigDecimal.apply)
                    .map(_.setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble)
                    .toArray
                val roundedVector = DenseVector(roundedData)
                (index, roundedVector)
            }.collect
            .toSet
        val expectedEmbeddings = TestData.exampleSentenceEmbeddingTuples.toSet
        sentenceEmbeddings shouldEqual expectedEmbeddings
    }

    they should "be computed from the input data" in {
        val job = new SentenceEmbeddings
        job.embeddingStreamFunction = TextTestData.docfreqStream("wordembeddings_example", "/sentenceembeddings/")
        job.tokenizedSentences = sc.parallelize(TestData.exampleTokenizedSentences)
        job.stopwords = sc.parallelize(List.empty[String])
        job.wordFrequencies = sc.parallelize(TestData.exampleWordFrequencies)
        job.run(sc)
        val sentenceEmbeddings = job.sentenceEmbeddings.collect.toList
        sentenceEmbeddings should have length 3
        sentenceEmbeddings.foreach { case (index, vector) => vector should have length 4 }
    }
}
