package de.hpi.ingestion.versioncontrol

import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.versioncontrol.models.HistoryEntry
import play.api.libs.json.{JsValue, Json}
import scala.io.Source

object TestData {

	val timeFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSZZZZZ")

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
				name_history = versions().head,
				aliases_history = versions()(1),
				category_history = versions()(2),
				properties_history = Map("test property" -> versions()(3)),
				relations_history = Map(
					UUID.fromString("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252") -> Map("test relation" -> versions()(4)))),
			Subject(
				id = UUID.fromString("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252"),
				name_history = versions()(4),
				aliases_history = versions()(4),
				category_history = versions()(4),
				properties_history = Map("test property" -> versions().head),
				relations_history = Map(
					UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989") -> Map("test relation" -> versions()(1)))))
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
				dataLists()(1),
				dataLists()(2),
				Map("test property" -> dataLists()(3)),
				Map(UUID.fromString("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252") -> Map("test relation" -> dataLists()(4)))),
			HistoryEntry(
				UUID.fromString("0bb8d6e1-998d-4a8b-8516-c68a4cb4c252"),
				None,
				None,
				None,
				Map("test property" -> dataLists().head),
				Map(UUID.fromString("465b3a7a-c621-42ad-a4f2-34e229602989") -> Map("test relation" -> dataLists()(1)))))
	}
}
