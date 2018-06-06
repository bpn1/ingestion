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

package de.hpi.ingestion.deduplication.similarity

/**
  * Provides a method to measure the similarity of two objects
  * @tparam T the type of the objects to be compared
  */
trait SimilarityMeasure[-T] extends Serializable {

    /**
      * Calculates a similarity score for two objects
      * @param x object to be compared to y
      * @param y object to be compared to x
      * @param u is for giving config params to similarity measures that take one
      * @return a normalized similarity score between 1.0 and 0.0
      */
    def compare(x: T, y: T, u: Int) : Double
}

/**
  * Companion-object to the Similarity Measure trait providing easy to use class reflection to get a Similarity Measure
  * by its name.
  */
object SimilarityMeasure {
    val dataTypes: Map[String, SimilarityMeasure[_]] = Map(
        "ExactMatch" -> ExactMatch,
        "MongeElkan" -> MongeElkan,
        "Jaccard" -> Jaccard,
        "DiceSorensen" -> DiceSorensen,
        "Jaro" -> Jaro,
        "JaroWinkler" -> JaroWinkler,
        "N-Gram" -> NGram,
        "Overlap" -> Overlap,
        "EuclidianDistance" -> EuclidianDistance,
        "RelativeNumbersSimilarity" -> RelativeNumbersSimilarity,
        "UrlCompare" -> UrlCompare
    )

    /**
      * Returns a Similarity Measure given its name. If there is no Similarity Measure with the given name then
      * the default Similarity Measure Exact Match String is returned.
      * @param similarityMeasure name of the Similarity Measure
      * @tparam T type of the Similarity Measure
      * @return the requested Similarity Measure if it exists or else Exact Match String as default
      */
    def get[T](similarityMeasure: String): SimilarityMeasure[T] = {
        dataTypes.getOrElse(similarityMeasure, ExactMatch).asInstanceOf[SimilarityMeasure[T]]
    }
}
