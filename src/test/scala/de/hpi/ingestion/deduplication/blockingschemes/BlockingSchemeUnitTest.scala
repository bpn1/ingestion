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

package de.hpi.ingestion.deduplication.blockingschemes

import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.deduplication.TestData
import org.scalatest.{FlatSpec, Matchers}

class BlockingSchemeUnitTest extends FlatSpec with Matchers {
    "SimpleBlockingScheme" should "generate proper keys" in {
        val subjects = TestData.subjects
        val blockingScheme = SimpleBlockingScheme("Test SimpleBS")
        val keys = subjects.map(blockingScheme.generateKey)
        val expected = TestData.simpleBlockingScheme
        keys.toSet shouldEqual expected.toSet
    }

    it should "generate a default undefined key if there is no name" in {
        val blockingScheme = SimpleBlockingScheme("Test SimpleBS")
        val subject = Subject(master = null, datasource = null)
        val key = blockingScheme.generateKey(subject)
        key shouldEqual List(blockingScheme.undefinedValue)
    }

    it should "ignore 'The ' at the beginning of the name when generating a key" in {
        val subjects = TestData.subjectsStartingWithThe
        val blockingScheme = SimpleBlockingScheme("Test SimpleBS")
        val keys = subjects.map(blockingScheme.generateKey)
        val expected = TestData.simpleBlockingSchemeWithThe
        keys.toSet shouldEqual expected.toSet
    }

    "LastLettersBlockingScheme" should "generate proper keys" in {
        val subjects = TestData.subjects :+ Subject(master = null, datasource = null)
        val blockingScheme = LastLettersBlockingScheme("Test LastLettersBS")
        val keys = subjects.map(blockingScheme.generateKey)
        val expected = TestData.lastLettersBlockingScheme
        keys.toSet shouldEqual expected.toSet
    }

    "ListBlockingScheme" should "generate proper keys" in {
        val subjects = TestData.subjects
        val blockingScheme = ListBlockingScheme("Test ListBS", "geo_city", "gen_income")
        val keys = subjects.map(blockingScheme.generateKey)
        val expected = TestData.listBlockingScheme
        keys.toSet shouldEqual expected.toSet
    }

    "MappedListBlockingScheme" should "generate proper keys" in {
        val subjects = TestData.subjects
        val function: String => String = attribute => attribute.substring(0, Math.min(3, attribute.length))
        val blockingScheme = MappedListBlockingScheme("Test MapBS", function, "name")
        val keys = subjects.map(blockingScheme.generateKey)
        val expected = TestData.mapBlockingScheme
        keys.toSet shouldEqual expected.toSet
    }

    it should "behave like ListBlockingScheme if no function is given" in {
        val subjects = TestData.subjects
        val attribute = "geo_city"
        val blockingScheme = MappedListBlockingScheme("Test MapBS", identity, attribute)
        val listBlockingScheme = ListBlockingScheme("Test ListBS", attribute)
        subjects
            .map(subject => (blockingScheme.generateKey(subject), listBlockingScheme.generateKey(subject)))
            .foreach { case (keys, expected) =>
                keys shouldEqual expected
            }
    }

    "GeoCoordsBlockingScheme" should "generate proper keys" in {
        val subjects = TestData.subjects
        val blockingScheme = GeoCoordsBlockingScheme("Test GeoCoordsBS")
        val keys = subjects.map(blockingScheme.generateKey)
        val expected = TestData.geoCoordsBlockingSchemeDefault
        keys.toSet shouldEqual expected.toSet
    }

    it should "let you adjust the number of decimal places" in {
        val subjects = TestData.subjects
        val blockingScheme = GeoCoordsBlockingScheme("Test GeoCoordsBS", 2)
        val keys = subjects.map(blockingScheme.generateKey)
        val expected = TestData.geoCoordsBlockingSchemeDecimals
        keys.toSet shouldEqual expected.toSet
    }

    "RandomBlockingScheme" should "generate random keys from the UUIDs" in {
        val subjects = TestData.subjects
        val blockingScheme = new RandomBlockingScheme
        val keys = subjects.map(blockingScheme.generateKey)
        val expected = TestData.randomBlockingScheme
        keys.toSet shouldEqual expected.toSet
    }

    it should "be created with the proper tag" in {
        val name = "Test Random Scheme"
        val scheme = RandomBlockingScheme(name)
        scheme.tag shouldEqual name
        scheme.isInstanceOf[RandomBlockingScheme] shouldBe true
    }
}
