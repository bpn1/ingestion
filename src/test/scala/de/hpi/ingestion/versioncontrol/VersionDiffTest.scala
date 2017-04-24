package de.hpi.ingestion.versioncontrol

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import java.util.UUID

class VersionDiffTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {

    val oldVersion = UUID.fromString("fc2c5a40-c566-11e6-aee2-5f2c06e3b302")
    val newVersion = UUID.fromString("7ce032b0-c567-11e6-8252-5f2c06e3b302")

	"Version diff" should "be created" in {
		val subjects = sc.parallelize(TestData.diffSubjects())
		val (oldV, newV) = TestData.versionsToCompare()
		val versions = Array(oldV.toString, newV.toString)
		val versionDiff = VersionDiff.run(subjects, versions)
		val expectedDiff = sc.parallelize(TestData.jsonDiff())
		assertRDDEquals(versionDiff, expectedDiff)
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

	"History entry" should "be diffed and serialized as JSON" in {
		val diff = TestData.historyEntries()
			.map(VersionDiff.diffToJson)
		val expectedDiff = TestData.jsonDiff()
		diff shouldEqual expectedDiff
	}

	"Value lists" should "be created from two versions" in {
		val (oldV, newV) = TestData.versionsToCompare()
		val valueLists = TestData.versions()
			.map(VersionDiff.createValueList(oldV, newV, _))
		val expectedLists = TestData.dataLists()
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
}
