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

package de.hpi.ingestion.versioncontrol

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import java.util.UUID
import de.hpi.ingestion.versioncontrol.models.SubjectDiff

class VersionDiffTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {

    val oldVersion = UUID.fromString("fc2c5a40-c566-11e6-aee2-5f2c06e3b302")
    val newVersion = UUID.fromString("7ce032b0-c567-11e6-8252-5f2c06e3b302")

	"Version diff" should "be created" in {
		val job = new VersionDiff
		job.subjects = sc.parallelize(TestData.diffSubjects())
		val (oldV, newV) = TestData.versionsToCompare()
		job.args = Array(oldV.toString, newV.toString)
		job.run(sc)
		val versionDiff = job.subjectDiff.collect.toList
		val expectedDiff = TestData.subjectDiff()
		versionDiff shouldEqual expectedDiff
	}

	"Versions" should "be ordered correctly" in {
		val orderedVersions = TestData.unorderedVersions()
			.map((VersionDiff.versionOrder _).tupled)
		val expectedVersions = TestData.orderedVersions()
		orderedVersions shouldEqual expectedVersions
	}

	"Value lists of a Subject" should "be retrieved from the version lists" in {
		val (oldV, newV) = TestData.versionsToCompare()
		val retrievedData = TestData.diffSubjects()
			.map(VersionDiff.retrieveVersions(_, oldV, newV))
		val expectedData = TestData.historyEntries()
		retrievedData shouldEqual expectedData
	}

	"Correct values" should "be retrieved from version list" in {
		val (oldV, newV) = TestData.versionsToCompare()
		val fieldValues = TestData.versions()
			.map(versionList => (VersionDiff.findValues(oldV, versionList), VersionDiff.findValues(newV, versionList)))
		val expectedValues = TestData.foundValues()
		fieldValues shouldEqual expectedValues
	}

	"History entry" should "be diffed and written into SubjectDiff" in {
		val (oldV, newV) = TestData.versionsToCompare()
		val diff = TestData.historyEntries()
			.map(VersionDiff.historyToDiff(_, oldV, newV))
		val expectedDiff = TestData.subjectDiff()
		diff shouldEqual expectedDiff
	}

	it should "contain the master diff" in {
		val (oldV, newV) = TestData.versionsToCompare()
		val diff = TestData.historyEntriesWithMaster()
			.map(VersionDiff.historyToDiff(_, oldV, newV))
		val expectedDiff = TestData.subjectDiffWithMaster()
		diff shouldEqual expectedDiff
	}

	"Value lists" should "be created from two versions" in {
		val (oldV, newV) = TestData.versionsToCompare()
		val valueLists = TestData.versions()
			.map(VersionDiff.createValueList(oldV, newV, _))
		val expectedLists = TestData.dataLists()
		valueLists shouldEqual expectedLists
	}

	"Value lists" should "be created when the versions are the same" in {
		val oldV = TestData.versionsToCompare()._1
		val valueLists = TestData.versions()
			.map(VersionDiff.createValueList(oldV, oldV, _))
		val expectedLists = TestData.sameVersionDataLists()
		valueLists shouldEqual expectedLists
	}

	they should "be diffed and serialized as JSON" in {
		val valueDiffs = TestData.dataLists()
			.map(VersionDiff.diffLists)
		val expectedDiffs = TestData.listDiffs()
		valueDiffs shouldEqual expectedDiffs
	}

	"Time" should "be extracted from UUID" in {
		val uuidTimes = TestData.timeUUIDs()
			.map(VersionDiff.timeFromUUID)
		val expectedTimes = TestData.timeOfTimeUUIDs()
		uuidTimes shouldEqual expectedTimes
	}

	"Version Diff assertion" should "assert that there are two versions provided" in {
		val job = new VersionDiff
		job.args = Array("v1", "v2")
		job.assertConditions() shouldBe true
		job.args = Array("v1")
		job.assertConditions() shouldBe false
	}
}
