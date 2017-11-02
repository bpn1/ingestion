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

import com.rockymadden.stringmetric.similarity.JaccardMetric

/**
  * A hybrid similarity measure comparing strings corresponding to the jaccard algorithm
  */
object Jaccard extends SimilarityMeasure[String] {

    /**
      * Calculates the jaccard similarity score for two strings
      * @param s string to be compared to t
      * @param t string to be compared to s
      * @param u specifies the n-gram in the algorithm
      * @return a normalized similarity score between 1.0 and 0.0 or
      * the default value 0.0 if one of the input strings is empty
      */
    override def compare(s: String, t: String, u: Int = 1) : Double = {
        val score = JaccardMetric(u).compare(s,t)
        score.getOrElse(0.0)
    }
}
