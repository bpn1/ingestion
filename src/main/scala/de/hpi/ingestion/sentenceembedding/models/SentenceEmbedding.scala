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

package de.hpi.ingestion.sentenceembedding.models

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.{Vectors, Vector => MLVector}

/**
  * Represents a sentence embedding.
  * @param id index of the sentence used to identify it
  * @param sentence the text used to compute the sentence embedding
  * @param embedding the computed sentence embedding
  * @param cluster the cluster the sentence embedding was assigned to
  */
case class SentenceEmbedding(
    id: Long,
    sentence: Option[String] = None,
    embedding: List[Double] = Nil,
    cluster: Option[Int] = None,
    tokens: List[String] = Nil
) {
    /**
      * Transforms this sentence embedding to a Breeze dense vector.
      * @return Breeze dense vector containing the sentence embedding
      */
    def toBreezeVector: DenseVector[Double] = {
        DenseVector(embedding.toArray)
    }

    /**
      * Transforms this sentence embedding to a Spark dense vector.
      * @return Spark vector containing the sentence embedding
      */
    def toSparkVector: MLVector = {
        Vectors.dense(embedding.toArray)
    }
}
