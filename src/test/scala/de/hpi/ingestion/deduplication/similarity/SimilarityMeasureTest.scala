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

import org.scalatest.{FlatSpec, Matchers}

class SimilarityMeasureTest extends FlatSpec with Matchers {

    "Similarity Measure" should "be returned given its name" in {
        SimilarityMeasure.get[String]("ExactMatchString") shouldEqual ExactMatchString
        SimilarityMeasure.get[Double]("ExactMatchDouble") shouldEqual ExactMatchDouble
        SimilarityMeasure.get[String]("MongeElkan") shouldEqual MongeElkan
        SimilarityMeasure.get[String]("Jaccard") shouldEqual Jaccard
        SimilarityMeasure.get[String]("DiceSorensen") shouldEqual DiceSorensen
        SimilarityMeasure.get[String]("Jaro") shouldEqual Jaro
        SimilarityMeasure.get[String]("JaroWinkler") shouldEqual JaroWinkler
        SimilarityMeasure.get[String]("N-Gram") shouldEqual NGram
        SimilarityMeasure.get[String]("Overlap") shouldEqual Overlap
        SimilarityMeasure.get[String]("EuclidianDistance") shouldEqual EuclidianDistance
        SimilarityMeasure.get[String]("RelativeNumbersSimilarity") shouldEqual RelativeNumbersSimilarity
        SimilarityMeasure.get[String]("Not existing") shouldEqual ExactMatchString
    }
}
