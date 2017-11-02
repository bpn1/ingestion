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

package de.hpi.ingestion.textmining.preprocessing

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.textmining.TestData
import de.hpi.ingestion.textmining.models.FeatureEntry
import org.scalatest.{FlatSpec, Matchers}

class SecondOrderFeatureGeneratorTest extends FlatSpec with SharedSparkContext with Matchers {
    "Rank" should "be computed correctly" in {
        val values = TestData.valuesList()
        val ranks = SecondOrderFeatureGenerator.computeRanks(values)
        ranks shouldEqual TestData.ranksList()
    }

    it should "be computed correctly for a longer example" in {
        val values = TestData.longValuesList()
        val ranks = SecondOrderFeatureGenerator.computeRanks(values)
        ranks shouldEqual TestData.longRanksList()
    }

    "Difference to highest value (delta top)" should "be computed correctly" in {
        val values = TestData.valuesList()
        val deltaTops = SecondOrderFeatureGenerator.computeDeltaTopValues(values)
        deltaTops shouldEqual TestData.deltaTopValuesList()
    }

    "Difference to next value (delta successor)" should "be computed correctly" in {
        val values = TestData.valuesList()
        val deltaTops = SecondOrderFeatureGenerator.computeDeltaSuccessorValues(values)
        deltaTops shouldEqual TestData.deltaSuccessorValuesList()
    }

    "Feature entries with second order features" should "be exactly these feature entries" in {
        val job = new SecondOrderFeatureGenerator
        job.featureEntries = sc.parallelize(TestData.featureEntriesList())
        job.run(sc)
        val featureEntriesSet = job.featureEntriesWithSOF.collect.toSet
        featureEntriesSet shouldEqual TestData.featureEntriesWitSOFSet()

        job.featureEntries = sc.parallelize(TestData.featureEntriesForSingleAliasList())
        job.run(sc)
        var featureEntriesList = job.featureEntriesWithSOF
            .collect
            .toList
            .sortBy(featureEntry => (featureEntry.article, featureEntry.offset, featureEntry.entity))
        featureEntriesList shouldEqual TestData.featureEntriesForSingleAliasWithSOFList()

        job.featureEntries = sc.parallelize(TestData.featureEntriesForManyPossibleEntitiesList())
        job.run(sc)
        featureEntriesList = job.featureEntriesWithSOF
            .collect
            .toList
            .sortBy(featureEntry => (featureEntry.entity_score.rank, featureEntry.entity))
        featureEntriesList shouldEqual TestData.featureEntriesForManyPossibleEntitiesWithSOFList()
    }
}
