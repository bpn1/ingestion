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

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class DependencyTreeStatisticsTest extends FlatSpec with Matchers with SharedSparkContext {
    "Dependency parse trees" should "be counted" in {
        val job = new DependencyTreeStatistics
        job.dependencyTrees = sc.parallelize(TestData.dependencyTrees)
        job.run(sc)
        val patternFrequencies = job.patternFrequencies.collect.toSet
        val expectedFrequencies = TestData.dependecyTreeFrequencies
        patternFrequencies shouldEqual expectedFrequencies
    }

    they should "be aggregated" in {
        val job = new DependencyTreeStatistics
        job.dependencyTrees = sc.parallelize(TestData.dependencyTrees)
        job.run(sc)
        val aggregatedSentences = job.aggregatedSentences
            .collect
            .map(patterns => patterns.copy(sentences = patterns.sentences.sorted))
            .toSet
        val expectedAggregations = TestData.aggregatedDependencyTrees
        aggregatedSentences shouldEqual expectedAggregations
    }
}
