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

import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.versioncontrol.models.{HistoryEntry, SubjectDiff}
import play.api.libs.json.{JsValue, Json}
import scala.io.Source

// scalastyle:off line.size.limit
object TestData {

	val timeFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSZZZZZ")
	val datasource = "testsource"

	def dateOf(date: String): Date = timeFormatter.parse(date)

	def timeUUIDs(): List[UUID] = {
		List(
			UUID.fromString("224e1a50-13e2-11e7-9a30-7384674b582f"),
			UUID.fromString("2195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
			UUID.fromString("7b410340-243e-11e7-937a-ad9adce5e136"),
			UUID.fromString("f44df8b0-2425-11e7-aec2-2d07f82c7921"))
	}

	def timeOfTimeUUIDs(): List[Long] = {
		List(
			1490724761717L,
			1487518996919L,
			1492523643252L,
			1492513108923L)
	}

	def testVersion(version: String, value: List[String], timestamp: String): Version = {
		val templateVersion = Version(null, "test program", Nil, Map[String, String](), Nil, null)
		templateVersion.copy(version = UUID.fromString(version), value = value, timestamp = dateOf(timestamp))
	}

	def versionsToCompare(): (UUID, UUID) = {
		(timeUUIDs()(1), timeUUIDs()(3))
	}

	def versions(): List[List[Version]] = {
		List(
			List(
				testVersion("710b962e-041c-11e1-9234-0123456789ab", List("very old"), "2011-11-01 00:00:00.000+0000"),
				testVersion("2195bc70-f6ba-11e6-aa16-63ef39f49c5d", List("a", "b"), "2017-02-19 15:43:16.912+0000"),
				testVersion("224e1a50-13e2-11e7-9a30-7384674b582f", List("b", "c"), "2017-03-28 18:12:41.710+0000")),
			List(testVersion("2195bc70-f6ba-11e6-aa16-63ef39f49c5d", List("a"), "2017-02-19 15:43:16.912+0000"),
				testVersion("f44df8b0-2425-11e7-aec2-2d07f82c7921", Nil, "2017-04-18 10:58:28.923+0000"),
				testVersion("7b410340-243e-11e7-937a-ad9adce5e136", List("c"), "2017-04-18 13:54:03.253+0000")),
			List(testVersion("2195bc70-f6ba-11e6-aa16-63ef39f49c5d", Nil, "2017-02-19 15:43:16.912+0000"),
				testVersion("224e1a50-13e2-11e7-9a30-7384674b582f", List("a", "b"), "2017-03-28 18:12:41.710+0000"),
				testVersion("f44df8b0-2425-11e7-aec2-2d07f82c7921", List("b"), "2017-04-18 10:58:28.923+0000")),
			List(testVersion("2195bc70-f6ba-11e6-aa16-63ef39f49c5d", List("a", "b"), "2017-02-19 15:43:16.912+0000"),
				testVersion("224e1a50-13e2-11e7-9a30-7384674b582f", Nil, "2017-03-28 18:12:41.710+0000"),
				testVersion("f44df8b0-2425-11e7-aec2-2d07f82c7921", List("a", "b"), "2017-04-18 10:58:28.923+0000")),
			List(testVersion("224e1a50-13e2-11e7-9a30-7384674b582f", List("b"), "2017-03-28 18:12:41.710+0000"),
				testVersion("f44df8b0-2425-11e7-aec2-2d07f82c7921", Nil, "2017-04-18 10:58:28.923+0000"),
				testVersion("7b410340-243e-11e7-937a-ad9adce5e136", List("a"), "2017-04-18 13:54:03.253+0000"))
		)
	}

	def futureVersions(): List[Version] = {
		List(testVersion("f44df8b0-2425-11e7-aec2-2d07f82c7921", List("new"), "2017-04-18 10:58:28.923+0000"),
			testVersion("f44df8b0-2425-11e7-aec2-2d07f82c7921", Nil, "2017-04-18 10:58:28.923+0000"),
			testVersion("f44df8b0-2425-11e7-aec2-2d07f82c7921", List("new"), "2017-04-18 10:58:28.923+0000"))
	}

	def masterVersions(): List[Version] = {
		List(
			testVersion("710b962e-041c-11e1-9234-0123456789ab", List("465b3a7a-c621-42ad-a4f2-34e229602989"),
				"2011-11-01 00:00:00.000+0000"),
			testVersion("2195bc70-f6ba-11e6-aa16-63ef39f49c5d", List("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252"),
				"2017-02-19 15:43:16.912+0000"),
			testVersion("f44df8b0-2425-11e7-aec2-2d07f82c7921", Nil, "2017-04-18 10:58:28.923+0000"))
	}

	def masterValuesVersions(): List[Version] = {
		List(
			testVersion("710b962e-041c-11e1-9234-0123456789ab", List("0.8"),
				"2011-11-01 00:00:00.000+0000"),
			testVersion("2195bc70-f6ba-11e6-aa16-63ef39f49c5d", List("1.0"),
				"2017-02-19 15:43:16.912+0000"),
			testVersion("f44df8b0-2425-11e7-aec2-2d07f82c7921", Nil, "2017-04-18 10:58:28.923+0000"))
	}

	def foundValues(): List[(List[String], List[String])] = {
		List(
			(List("a", "b"), List("b", "c")),
			(List("a"), Nil),
			(Nil, List("b")),
			(List("a", "b"), List("a", "b")),
			(Nil, Nil))
	}

	def unorderedVersions(): List[(UUID, UUID)] = {
		List(
			(timeUUIDs().head, timeUUIDs()(1)),
			(timeUUIDs()(1), timeUUIDs().head),
			(timeUUIDs()(2), timeUUIDs()(2)),
			(timeUUIDs()(3), timeUUIDs()(1)),
			(timeUUIDs()(2), timeUUIDs()(3)))
	}

	def orderedVersions(): List[(UUID, UUID)] = {
		List(
			(timeUUIDs()(1), timeUUIDs().head),
			(timeUUIDs()(1), timeUUIDs().head),
			(timeUUIDs()(2), timeUUIDs()(2)),
			(timeUUIDs()(1), timeUUIDs()(3)),
			(timeUUIDs()(3), timeUUIDs()(2)))
	}

	def dataLists(): List[Option[(List[String], List[String])]] = {
		List(
			Option((List("a", "b"), List("b", "c"))),
			Option((List("a"), Nil)),
			Option((Nil, List("b"))),
			Option((List("a", "b"), List("a", "b"))),
			None)
	}

	def sameVersionDataLists(): List[Option[(List[String], List[String])]] = {
		List(
			Option((Nil, List("a", "b"))),
			Option((Nil, List("a"))),
			None,
			Option((Nil, List("a", "b"))),
			None)
	}

	def listDiffs(): List[Option[JsValue]] = {
		List(
			Option(Json.parse("""{"+": ["c"], "-": ["a"]}""")),
			Option(Json.parse("""{"-": ["a"]}""")),
			Option(Json.parse("""{"+": ["b"]}""")),
			None,
			None)
	}

	def diffSubjects(): List[Subject] = {
		List(
			Subject(
				id = UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989"),
				master = UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989"),
				datasource = datasource,
				name_history = versions().head,
				aliases_history = versions()(1),
				category_history = versions()(2),
				properties_history = Map("test property" -> versions()(3)),
				relations_history = Map(
					UUID.fromString("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252") -> Map("test relation" -> versions()(4))
				)
			),
			Subject(
				id = UUID.fromString("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252"),
				master = UUID.fromString("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252"),
				datasource = datasource,
				name_history = versions()(4),
				aliases_history = versions()(4),
				category_history = versions()(4),
				properties_history = Map("test property" -> versions().head),
				relations_history = Map(
					UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989") -> Map("test relation" -> versions()(1))
				)
			)
		)
	}

	def additionalRestorationSubjects(): List[Subject] = {
		List(
			Subject(
				id = UUID.fromString("d34db33f-c621-42ad-a4f2-34e229602989"),
				master = UUID.fromString("d34db33f-c621-42ad-a4f2-34e229602989"),
				datasource = datasource,
				name_history = futureVersions(),
				aliases_history = futureVersions(),
				category_history = futureVersions(),
				properties_history = Map("test property" -> futureVersions()),
				relations_history = Map(UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989")
					-> Map("test relation" -> futureVersions()))),
			Subject(
				id = UUID.fromString("10200a26-a66a-4405-aab1-4977852e02a9"),
				master = UUID.fromString("10200a26-a66a-4405-aab1-4977852e02a9"),
				datasource = datasource,
				master_history = masterVersions(),
				relations_history = Map(
					UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989")
						-> Map("master" -> List(masterValuesVersions().head)),
					UUID.fromString("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252")
						-> Map("master" -> List(masterValuesVersions()(1))))))
	}

	def restoredSubjects(): List[Subject] = List(
		Subject(
			id = UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989"),
			master = UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989"),
			datasource = datasource,
			name = foundValues().head._1.headOption,
			aliases = foundValues()(1)._1,
			category = foundValues()(2)._1.headOption,
			properties = Map("test property" -> foundValues()(3)._1),
			name_history = versions().head,
			aliases_history = versions()(1),
			category_history = versions()(2),
			properties_history = Map("test property" -> versions()(3)),
			relations_history = Map(
				UUID.fromString("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252") -> Map("test relation" -> versions()(4)))),
		Subject(
			id = UUID.fromString("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252"),
			master = UUID.fromString("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252"),
			datasource = datasource,
			name = foundValues()(4)._1.headOption,
			aliases = foundValues()(4)._1,
			category = foundValues()(4)._1.headOption,
			properties = Map("test property" -> foundValues().head._1),
			relations = Map(
				UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989") ->
					Map("test relation" -> foundValues()(1)._1.headOption.getOrElse(""))),
			name_history = versions()(4),
			aliases_history = versions()(4),
			category_history = versions()(4),
			properties_history = Map("test property" -> versions().head),
			relations_history = Map(
				UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989") -> Map("test relation" -> versions()(1)))),
		Subject(
			id = UUID.fromString("d34db33f-c621-42ad-a4f2-34e229602989"),
			master = UUID.fromString("d34db33f-c621-42ad-a4f2-34e229602989"),
			datasource = datasource,
			name_history = futureVersions(),
			aliases_history = futureVersions(),
			category_history = futureVersions(),
			properties_history = Map("test property" -> versions().head),
			relations_history = Map(UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989")
				-> Map("test relation" -> futureVersions()))),
		Subject(
			id = UUID.fromString("10200a26-a66a-4405-aab1-4977852e02a9"),
			master = UUID.fromString("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252"),
			datasource = datasource,
			relations = Map(
				UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989") -> Map("master" -> "0.8"),
				UUID.fromString("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252") -> Map("master" -> "1.0")),
			master_history = masterVersions())
	)

	def subjectDiff(): List[SubjectDiff] = {
		val (oldV, newV) = TestData.versionsToCompare()
		List(
			SubjectDiff(
				oldV, newV, UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989"),
				name = Option("{\"+\":[\"c\"],\"-\":[\"a\"]}"),
				aliases = Option("{\"-\":[\"a\"]}"),
				category = Option("{\"+\":[\"b\"]}")),
			SubjectDiff(
				oldV, newV, UUID.fromString("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252"),
				properties = Option("{\"test property\":{\"+\":[\"c\"],\"-\":[\"a\"]}}"),
				relations = Option("{\"465b3a7a-c621-42ad-a4f2-34e229602989\":{\"test relation\":{\"-\":[\"a\"]}}}"))
		)
	}

	def jsonDiff(): List[JsValue] = {
		val rawJson = Source.fromURL(getClass.getResource("/versioncontrol/diff.json")).getLines().mkString("\n")
		Json.parse(rawJson).as[List[JsValue]]
	}

	def historyEntries(): List[HistoryEntry] = {
		List(
			HistoryEntry(
				UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989"),
				dataLists().head,
				None,
				dataLists()(1),
				dataLists()(2),
				Map("test property" -> dataLists()(3)),
				Map(UUID.fromString("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252") -> Map("test relation" -> dataLists()(4)))),
			HistoryEntry(
				UUID.fromString("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252"),
				None,
				None,
				None,
				None,
				Map("test property" -> dataLists().head),
				Map(UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989") -> Map("test relation" -> dataLists()(1)))))
	}

	def historyEntriesWithMaster(): List[HistoryEntry] = {
		List(
			HistoryEntry(
				id = UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989"),
				master = Option((List("465b3a7a-c621-42ad-a4f2-34e229602989"), List("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252")))
			)
		)
	}

	def subjectDiffWithMaster(): List[SubjectDiff] = {
		val (oldV, newV) = TestData.versionsToCompare()
		List(
			SubjectDiff(
				oldV, newV, UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989"),
				master = Option("{\"+\":[\"0bb8d6e1-998d-4a8b-8516-c68a4cb4c252\"],\"-\":[\"465b3a7a-c621-42ad-a4f2-34e229602989\"]}")
			)
		)
	}
}
// scalastyle:on line.size.limit
