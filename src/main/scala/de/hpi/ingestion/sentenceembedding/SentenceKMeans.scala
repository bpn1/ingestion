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
import de.hpi.ingestion.sentenceembedding.models.SentenceEmbedding
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.KMeans
import com.datastax.spark.connector._

/**
  * Clusters Sentence Embeddings with the k-means algorithm.
  */
class SentenceKMeans extends SparkJob {
    appName = "Sentence KMeans"
    configFile = "sentenceembeddings.xml"
    val defaultNumClusters = settings("kmeansClusters").toInt
    val defaultNumIterations = settings("kmeansIterations").toInt

    var sentenceEmbeddings: RDD[SentenceEmbedding] = _
    var sentenceEmbeddingsWithCluster: RDD[SentenceEmbedding] = _

    // $COVERAGE-OFF$
    /**
      * Loads the Sentence Embeddings from the Cassandra.
      * @param sc SparkContext to be used for the job
      */
    override def load(sc: SparkContext): Unit = {
        sentenceEmbeddings = sc.cassandraTable[SentenceEmbedding](
            settings("keyspace"),
            settings("sentenceEmbeddingTable"))
    }

    /**
      * Saves the clustered Sentence Embeddings to the Cassandra.
      * @param sc SparkContext to be used for the job
      */
    override def save(sc: SparkContext): Unit = {
        sentenceEmbeddingsWithCluster.saveToCassandra(settings("keyspace"), settings("sentenceEmbeddingTable"))
    }
    // $COVERAGE-ON$
    /**
      * Clusters the Sentence Embeddings using the k-means algorithm.
      * @param sc SparkContext to be used for the job
      */
    override def run(sc: SparkContext): Unit = {
        val parsedSentenceEmbeddings = sentenceEmbeddings
            .collect {
                case embedding if embedding.embedding.nonEmpty => (embedding, embedding.toSparkVector)
            }
        val trainEmbeddings = parsedSentenceEmbeddings.values.cache()
        val numClusters = defaultNumClusters
        val numIterations = defaultNumIterations
        val model = KMeans.train(trainEmbeddings, numClusters, numIterations)
        sentenceEmbeddingsWithCluster = parsedSentenceEmbeddings.map { case (embedding, vector) =>
            val cluster = model.predict(vector)
            embedding.copy(cluster = Option(cluster))
        }
    }
}
