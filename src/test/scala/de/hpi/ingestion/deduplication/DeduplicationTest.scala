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

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.{FlatSpec, Matchers}

class DeduplicationTest extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {
    "Compare" should "calculate a score regarding the configuration" in {
        val subject = TestData.subjects.head
        val staging = TestData.stagings.head
        val config = TestData.testConfig()
        val score = Deduplication.compare(subject, staging, config)
        val expected = TestData.testConfigScore
        score shouldEqual expected
    }

    it should "calculate the correct score for a complex configuration" in {
        val subject = TestData.subjects.head
        val staging = TestData.stagings.head
        val config = TestData.complexTestConfig
        val score = Deduplication.compare(subject, staging, config)
        val expected = TestData.complexTestConfigScore
        score shouldEqual expected
    }

    it should "just return the score if the configuration contains only one element" in {
        val subject = TestData.subjects.head
        val staging = TestData.stagings.head
        val config = TestData.simpleTestConfig
        val score = Deduplication.compare(subject, staging, config)
        val expected = TestData.simpleTestConfigScore
        score shouldEqual expected
    }

    "Duplicates" should "be found, filtered and returned with their score" in {
        val scoreConfig = sc.broadcast(TestData.testConfig())
        val duplicates = Deduplication.findDuplicates(TestData.subjectBlocks(sc), 0.9, scoreConfig)
        val expected = TestData.filteredDuplicates(sc)
        assertRDDEquals(duplicates, expected)
    }

    they should "be grouped into Duplicate Candidates" in {
        val subjects = TestData.subjects
        val stagings = TestData.stagings
        val duplicates = TestData.testDuplicates(sc)
        val candidates = Deduplication.createDuplicates(duplicates, "subject_wikidata")
        val expectedCandidates = sc.parallelize(TestData.createdDuplicateCandidates(subjects, stagings))
        assertRDDEquals(candidates, expectedCandidates)
    }

    "Deduplication" should "find duplicates and create duplicate candidates" in {
        val job = new Deduplication
        job.scoreConfigSettings = TestData.testConfig()
        job.settings = Map("stagingTable" -> "subject_wikidata", "confidence" -> "0.9", "maxBlockSize" -> "50000")
        val subjects = TestData.subjects
        val stagedSubjects = TestData.stagings
        val duplicates = TestData.trueDuplicates(subjects, stagedSubjects)
        job.subjects = sc.parallelize(subjects)
        job.stagedSubjects = sc.parallelize(stagedSubjects)
        job.run(sc)
        val expected = sc.parallelize(duplicates)
        assertRDDEquals(job.duplicates, expected)
    }
}
