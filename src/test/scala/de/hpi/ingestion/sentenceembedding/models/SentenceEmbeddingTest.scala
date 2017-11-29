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

import org.scalatest.{FlatSpec, Matchers}

class SentenceEmbeddingTest extends FlatSpec with Matchers {
    "Sentence Embedding" should "be converted Breeze vectors" in {
        val vectors = TestData.sentenceEmbeddings.map(_.toBreezeVector)
        val expectedVectors = TestData.breezeVectors
        vectors shouldEqual expectedVectors
    }

    they should "be converted to Spark vectors" in {
        val vectors = TestData.sentenceEmbeddings.map(_.toSparkVector)
        val expectedVectors = TestData.sparkVectors
        vectors shouldEqual expectedVectors
    }
}
