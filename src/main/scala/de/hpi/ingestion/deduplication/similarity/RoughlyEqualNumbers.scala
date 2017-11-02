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
  * A similarity measure that computes how close two given numbers are to each other in terms of percentage.
  */
object RelativeNumbersSimilarity extends SimilarityMeasure[String] {

    /**
      * Calculates how close are two given numbers to each other in terms of percentage
      * @param s double to be compared to t
      * @param t double to be compared to s
      * @param u has no specific use in here
      * @return a normalized similarity score between 1.0 and 0.0 or
      */
    override def compare(s: String, t: String, u: Int = 1) : Double = {
        val max = Math.max(s.toDouble, t.toDouble)
        val min = Math.min(s.toDouble, t.toDouble)
        min / max
    }
}
