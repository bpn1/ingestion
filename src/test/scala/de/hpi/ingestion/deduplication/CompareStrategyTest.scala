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

package de.hpi.ingestion.deduplication

import scala.math.BigDecimal
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.deduplication.models.config.AttributeConfig

class CompareStrategyTest extends FlatSpec with Matchers {
    "compare" should "return the score of the similarity of two strings" in {
        val config = TestData.testConfig()
        val subject = TestData.subjects.head
        val staging = TestData.stagings.head
        for {AttributeConfig(attribute, weight, scoreConfigs) <- config; scoreConfig <- scoreConfigs}{
            val score = scoreConfig.compare(subject.get(attribute).head, staging.get(attribute).head)
            val expected = TestData.testCompareScore(
                attribute,
                subject,
                staging,
                scoreConfig.similarityMeasure,
                scoreConfig
            )
            score shouldEqual expected
        }
    }

    "caseInsensitiveCompare" should "compare the values, ignoring upper case" in {
        val config = TestData.testConfig()
        val subject = TestData.subjects.head
        val staging = TestData.stagings.head
        for {AttributeConfig(attribute, weight, scoreConfigs) <- config; scoreConfig <- scoreConfigs}{
            val score = CompareStrategy.singleStringCompare(
                subject.get(attribute),
                staging.get(attribute),
                scoreConfig
            )
            val expected = TestData.testCompareScore(
                attribute,
                subject,
                staging,
                scoreConfig.similarityMeasure,
                scoreConfig
            )
            score shouldEqual expected
        }
    }

    "simpleStringCompare" should "only compare the first element in a list" in {
        val config = TestData.testConfig("geo_postal")
        val subject = TestData.subjects.head
        val staging = TestData.stagings.head
        for {AttributeConfig(attribute, weight, scoreConfigs) <- config; scoreConfig <- scoreConfigs}{
            val score = CompareStrategy.singleStringCompare(
                subject.get(attribute),
                staging.get(attribute),
                scoreConfig
            )
            val expected = TestData.testCompareScore(
                attribute,
                subject,
                staging,
                scoreConfig.similarityMeasure,
                scoreConfig
            )
            score shouldEqual expected
        }
    }

    "defaultCompare" should "compare each element from a list with each element from another" in {
        val config = TestData.testConfig("gen_sectors")
        val subject = TestData.subjects(2)
        val staging = TestData.subjects.last
        for {AttributeConfig(attribute, weight, scoreConfigs) <- config; scoreConfig <- scoreConfigs}{
            val score = BigDecimal(CompareStrategy.defaultCompare(
                subject.get(attribute),
                staging.get(attribute),
                scoreConfig
            )).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
            val expected = 1.0 * scoreConfig.weight
            score shouldEqual expected
        }
    }

    "apply" should "decide, which strategy should be used regarding the input attribute" in {
        val inputValues = TestData.testCompareInput
        val strategies = List(
            CompareStrategy.apply("name"),
            CompareStrategy.apply("geo_postal"),
            CompareStrategy.apply("gen_income")
        )
        val output = strategies.map(_.tupled(inputValues))
        val expected = TestData.expectedCompareStrategies.map(_.tupled(inputValues))
        output shouldEqual expected
    }
}
