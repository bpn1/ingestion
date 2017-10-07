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
import de.hpi.ingestion.deduplication.models.Duplicates
import org.scalatest.{FlatSpec, Matchers}

class DeduplicationTest extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {
	"Compare" should "calculate a score regarding the configuration" in {
		val job = new Deduplication
		val subject = TestData.subjects.head
		val staging = TestData.stagings.head
		val config = TestData.testConfig()
		val score = job.compare(subject, staging, config)
		val expected = TestData.testConfigScore
		score shouldEqual expected
	}

	it should "calculate the correct score for a complex configuration" in {
		val job = new Deduplication
		val subject = TestData.subjects.head
		val staging = TestData.stagings.head
		val config = TestData.complexTestConfig
		val score = job.compare(subject, staging, config)
		val expected = TestData.complexTestConfigScore
		score shouldEqual expected
	}

	it should "just return the score if the configuration contains only one element" in {
		val job = new Deduplication
		val subject = TestData.subjects.head
		val staging = TestData.stagings.head
		val config = TestData.simpleTestConfig
		val score = job.compare(subject, staging, config)
		val expected = TestData.simpleTestConfigScore
		score shouldEqual expected
	}

	"Duplicates" should "be found, filtered and returned with their score" in {
		val job = new Deduplication
		job.scoreConfigSettings = TestData.testConfig()
		job.settings = Map("confidence" -> "0.9")
		val duplicates = job.findDuplicates(TestData.subjectBlocks(sc), sc)
		val expected = TestData.filteredDuplicates(sc)
		assertRDDEquals(duplicates, expected)
	}

	they should "be grouped into Duplicate Candidates" in {
		val job = new Deduplication
		job.settings = Map("stagingTable" -> "subject_wikidata")
		val subjects = TestData.subjects
		val stagings = TestData.stagings
		val duplicates = TestData.testDuplicates(sc)
		val candidates = job.createDuplicates(duplicates)
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
