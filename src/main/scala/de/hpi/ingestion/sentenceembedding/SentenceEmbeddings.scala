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

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.StringImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import breeze.linalg._
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import com.datastax.spark.connector._
import de.hpi.ingestion.sentenceembedding.models.SentenceEmbedding

import scala.collection.mutable

/**
  * Calculates the sentence embeddings for given sentences and writes them to the Cassandra.
  */
class SentenceEmbeddings extends SparkJob {
    import SentenceEmbeddings._
    appName = "Sentence Embeddings"
    configFile = "sentenceembeddings.xml"
    sparkOptions("spark.kryoserializer.buffer.max") = "1024m"
    sparkOptions("spark.driver.maxResultSize") = "12g"

    val defaultStopwords = settings("stopwordFile")
    val defaultWordFrequencies = settings("wordFrequencyFile")
    val defaultWordEmbeddings = settings("wordEmbeddingFile")

    var weightParam = 1e-7
    var stopwords: RDD[String] = _
    var tokenizedSentences: RDD[SentenceEmbedding] = _
    var wordFrequencies: RDD[String] = _

    var sentenceEmbeddings: RDD[(Long, List[Double])] = _

    // $COVERAGE-OFF$
    var embeddingStreamFunction = hdfsFileStream _
    /**
      * Loads the stopwords, sentences and word frequencies from the HDFS.
      * @param sc SparkContext to be used for the job
      */
    override def load(sc: SparkContext): Unit = {
        stopwords = sc.textFile(conf.sentenceEmbeddingFiles.getOrElse("stopwords", defaultStopwords))
        wordFrequencies = sc.textFile(conf.sentenceEmbeddingFiles.getOrElse("wordfrequencies", defaultWordFrequencies))
        tokenizedSentences = sc.cassandraTable[SentenceEmbedding](
            settings("keyspace"),
            settings("sentenceEmbeddingTable"))
    }

    /**
      * Saves the sentence embeddings to the Cassandra.
      * @param sc SparkContext to be used for the job
      */
    override def save(sc: SparkContext): Unit = {
        sentenceEmbeddings.saveToCassandra(
            settings("keyspace"),
            settings("sentenceEmbeddingTable"),
            SomeColumns("id", "embedding"))
    }
    // $COVERAGE-ON$

    /**
      * Calculates the sentence embeddings of the given sentences.
      * @param sc SparkContext to be used for the job
      */
    override def run(sc: SparkContext): Unit = {
        // Set the weight parameter if the command line option was provided.
        setWeightParam()
        // Collect stopwords to implicitly broadcast them for the sentence preprocessing.
        val collectedStopwords = stopwords.collect().toSet
        // Calculate the word weights using the word frequencies and collect the weights for the calculation of the
        // sentence embeddings.
        val parsedWordFrequencies = wordFrequencies.flatMap(convertLineToWordFrequency)
        val totalWordFrequency = parsedWordFrequencies.values.sum
        val wordWeights = computeWordWeights(parsedWordFrequencies, totalWordFrequency)

        // Preprocess the sentences so that the sentence embeddings can be calculated. Zip them with their index so the
        // sentence embeddings can be correctly associated with the sentence they originate from.
        val indexedSentences = tokenizedSentences.map(sentence => (sentence.id, sentence.tokens))
        val preprocessedSentences = preprocessSentences(indexedSentences, collectedStopwords)

        // Calculate the sentence embedding using the fast text word embeddings. These are read from the HDFS by each
        // worker separately.
        val sentencesAsEmbeddings = computeSentenceEmbeddings(preprocessedSentences, wordWeights)

        // Remove the principal component from each sentence embedding and serialize the resulting sentence embeddings.
        val removedPrincipalComponent = removePrincipalComponent(sentencesAsEmbeddings)
        sentenceEmbeddings = removedPrincipalComponent.map { case (id, embedding) => (id, embedding.data.toList) }
    }

    /**
      * Sets the weight parameter if the command line option was provided.
      */
    def setWeightParam(): Unit = {
        conf.sentenceEmbeddingFiles.get("weightparam")
            .filter(_.isDigit)
            .map(_.toDouble)
            .foreach(weightParam = _)
    }

    /**
      * Calculates the word weights using the word frequencies and collects the weights for the calculation of the
      * sentence embeddings.
      * @param wordFrequencies RDD containing each word and its frequency
      * @param totalWordFrequency sum of all word frequencies
      * @return a collected map of each word and its weight
      */
    def computeWordWeights(wordFrequencies: RDD[(String, Int)], totalWordFrequency: Double): Map[String, Double] = {
        wordFrequencies
            .map { case (word, frequency) =>
                val weight = weightParam / (weightParam + (frequency.toDouble / totalWordFrequency))
                (word, weight)
            }.collect
            .toMap
    }



    /**
      * Calculate the sentence embedding using the fast text word embeddings. These are read from the HDFS by each
      * worker separately.
      * @param sentences RDD of preprocessed sentences, which are tokenized and cleaned, and their indices
      * @param wordWeights map of each word and its weight
      * @return RDD of each sentence embedding with its index
      */
    def computeSentenceEmbeddings(
        sentences: RDD[(Long, List[String])],
        wordWeights: Map[String, Double]
    ): RDD[(Long, DenseVector[Double])] = {
        sentences
            .mapPartitions({ partition =>
                val embeddingsMap = parseHDFSFileToMutableCollection(
                    conf.sentenceEmbeddingFiles.getOrElse("wordembeddings", defaultWordEmbeddings),
                    embeddingStreamFunction,
                    () => mutable.Map.empty[String, Array[Double]],
                    addWordEmbeddingFromFile)
                partition.flatMap { case (index, sentence) =>
                    val embeddings = convertSentenceToSentenceEmbedding(sentence, embeddingsMap, wordWeights)
                    embeddings.map((index, _))
                }
            }, true)
    }
}

object SentenceEmbeddings {
    /**
      * Parses a line containing a word and its embedding and adds it to a mutable map containing all word embeddings.
      * @param line the line containing the word and its embedding
      * @param embeddingsMap the mutable map containing every word and its parsed embedding
      */
    def addWordEmbeddingFromFile(line: String, embeddingsMap: mutable.Map[String, Array[Double]]): Unit = {
        val wordEmbeddingOption = convertLineToEmbedding(line)
        wordEmbeddingOption.foreach { case (word, wordEmbedding) =>
            embeddingsMap(word) = wordEmbedding
        }
    }

    /**
      * Takes a tokenized sentence as a list of tokens and optionally filters digits, stopwords and other specified
      * words and removes leading hashtags. Also transforms each token to its lower case version.
      * @param tokens list of tokens representing a sentence
      * @param stopwords Set of stopwords to filter
      * @param removeStopwords boolean flag indicating whether or not stopwords are filtered
      * @param removeDigits boolean flag indicating whether or not digits are filtered
      * @param removeHashtags boolean flag indicating whether or leading hashtags are removed
      * @param filterWords Set of other words that are filtered
      * @return list filtered, lower case tokens representing a preprocessed sentence
      */
    def preprocessSentences(
        tokens: List[String],
        stopwords: Set[String] = Set(),
        removeStopwords: Boolean = false,
        removeDigits: Boolean = false,
        removeHashtags: Boolean = true,
        filterWords: Set[String] = Set()
    ): List[String] = {
        tokens
            .map(_.toLowerCase)
            .map {
                case token if removeHashtags && token.startsWith("#") => token.slice(1, token.length)
                case token => token
            }.filter { token => !(token.isDigit && removeDigits) }
            .filter { token => !(stopwords(token) && removeStopwords) }
            .filter { token => !filterWords(token) }
    }

    /**
      * Preprocess the sentences so that the sentence embeddings can be calculated.
      * @param sentences RDD of indexed and tokenized sentences topreprocess
      * @param stopwords stopwords that will be removed from the tokenized sentences
      * @return RDD of tokenized and cleaned sentences and their index
      */
    def preprocessSentences(
        sentences: RDD[(Long, List[String])],
        stopwords: Set[String]
    ): RDD[(Long, List[String])] = {
        sentences
            .mapValues(preprocessSentences(_, stopwords, removeStopwords = true, removeDigits = true))
            .filter(_._2.nonEmpty)
    }

    /**
      * Transforms a line containing a word embedding into a tuple of the word and an array of the n-dimensional vector
      * containing the embedding.
      * @param line string containing the word and its embedding
      * @return a tuple of a word and its embedding or None if the line only contained two elements (such as the first
      *         line of the word embedding file)
      */
    def convertLineToEmbedding(line: String): Option[(String, Array[Double])] = {
        Option(line)
            .map(_.split(" "))
            .filter(_.length > 2)
            .map { splitLine =>
                val word = splitLine.head
                (word, splitLine.tail.map(_.toDouble))
            }
    }

    /**
      * Transforms a line containing a word frequency into a tuple of the word and its frequency.
      * @param line string containg the word and its frequency
      * @return a tuple of a word and its frequency or None if the line does not contain exactly two elements
      */
    def convertLineToWordFrequency(line: String): Option[(String, Int)] = {
        Option(line)
            .map(_.split("\t"))
            .filter(_.length == 2)
            .map { case Array(word, frequency) => (word, frequency.toInt) }
    }

    /**
      * Translate a sentence (list of tokens) into a sentence embedding. This sentence embedding only contains the word
      * for which a word embedding is found.
      * @param tokens list of tokens representing a sentence
      * @param wordEmbeddings map containing word embeddings used to look up the word embeddings of the sentence
      * @param wordWeights map containing word weights used to weigh the word embeddings
      * @return a Vector representing the sentence embedding or None if the sentence does not contain any word for which
      *         there is a word embedding
      */
    def convertSentenceToSentenceEmbedding(
        tokens: List[String],
        wordEmbeddings: mutable.Map[String, Array[Double]],
        wordWeights: Map[String, Double]
    ): Option[DenseVector[Double]] = {
        val weightedWordEmbeddings = tokens
            .flatMap { token =>
                val wordVector = wordEmbeddings.get(token)
                val wordWeight = wordWeights.getOrElse(token, 1.0)
                wordVector.map((_, wordWeight))
            }.map { case (wordVector, weight) =>
            DenseVector(wordVector) * weight
        }
        Option(weightedWordEmbeddings)
            .filter(_.nonEmpty)
            .map(_.reduce(_ + _))
    }

    /**
      * Remove the first principal component from each word embedding.
      * @param sentenceEmbeddings an RDD containing a sentence embedding and its index per row. This RDD represents a
      *                           matrix of the sentence embeddings.
      * @return an RDD containing a sentence embedding and its index per row
      */
    def removePrincipalComponent(
        sentenceEmbeddings: RDD[(Long, DenseVector[Double])]
    ): RDD[(Long, DenseVector[Double])] = {
        // Transform sentence embedding into an indexed row matrix to compute the SVD. The rows of the matrix should be
        // cached to greatly increase performance of the SVD computation.
        val indexedRows = sentenceEmbeddings.map { case (index, sentenceEmbedding) =>
            val vector = Vectors.dense(sentenceEmbedding.data)
            IndexedRow(index, vector)
        }.cache()
        val indexedRowMatrix = new IndexedRowMatrix(indexedRows)
        val svd = indexedRowMatrix.computeSVD(1)
        // Invert principal component and transform it into a breeze matrix to use it for further calculations.
        val principalComponent = svd.V
        val numCols = principalComponent.numCols
        val principalComponentRows = principalComponent
            .transpose
            .toArray
            .map(_ * -1.0)
            .grouped(numCols)
            .toList
        val principalComponentBreeze = DenseMatrix(principalComponentRows:_*)
        // Project every sentence embedding by the principal component to create a scaling factor for the principal
        // component.
        val projection = indexedRowMatrix.rows.map { indexedRow =>
            val index = indexedRow.index
            val sentenceEmbedding = DenseVector(indexedRow.vector.toArray)
            val scalingFactorVector = principalComponentBreeze.t * sentenceEmbedding
            val scalingFactor = scalingFactorVector.data.head
            (index, scalingFactor)
        }
        // Scale the principal component by the projected scalar to subtract them from the sentence embeddings.
        val scaling = projection.map { case (index, scalingFactor) =>
            val scaledPrincipalComponentsMatrix = scalingFactor * principalComponentBreeze
            val scaledPrincipalComponents = DenseVector(scaledPrincipalComponentsMatrix.toArray)
            (index, scaledPrincipalComponents)
        }
        // Subtract the scaled principal components from the sentence embeddings.
        val sentencesWithoutPrincipalComponent = indexedRowMatrix.rows
            .map { indexedRow =>
                val index = indexedRow.index
                val sentenceEmbedding = DenseVector(indexedRow.vector.toArray)
                (index, sentenceEmbedding)
            }.join(scaling)
            .map { case (id, (sentenceEmbedding, scaledPrincipalComponent)) =>
                (id, sentenceEmbedding - scaledPrincipalComponent)
            }.sortBy(_._1)
        sentencesWithoutPrincipalComponent
    }
}
