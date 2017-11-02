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


package de.hpi.ingestion.deduplication.models.config

import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure

/**
  * Reproduces a configuration for a comparison of two Subjects
  * @param similarityMeasure the similarity measure used for the comparison
  * @param weight the weight of the result
  * @param scale specifies the n-gram if the similarity measure has one
  * @tparam A type of the attribute
  * @tparam B type of the similarity measure
  */
case class SimilarityMeasureConfig[A, B <: SimilarityMeasure[A]](
    similarityMeasure: B,
    weight: Double = 0.0,
    scale: Int = 1
) extends WeightedFeatureConfig{
    override type T = SimilarityMeasureConfig[A, B]
    override def updateWeight(weight: Double): SimilarityMeasureConfig[A, B] = this.copy(weight = weight)

    /**
      * This method simply compares two strings.
      *
      * @param leftValue  String to be compared with rightValue.
      * @param rightValue String to be compared with leftValue.
      * @return The similarity score of the two input strings.
      */
    def compare(leftValue: A, rightValue: A): Double = {
        val score = similarityMeasure.compare(leftValue, rightValue, scale)
        score * weight
    }
}
