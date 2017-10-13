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

package de.hpi.ingestion.datalake

import java.util.UUID
import de.hpi.ingestion.datalake.mock.Entity
import de.hpi.ingestion.datalake.models.{Subject, Version}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

// scalastyle:off line.size.limit
// scalastyle:off method.length
object TestData {
	val datasource = "testSource"
	def subject: Subject = Subject.empty(datasource = datasource)

	def masterIds(): List[UUID] = {
		List(
			UUID.fromString("4fbc0340-4862-431f-9c28-a508234b8130"),
			UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d")
		)
	}

	def testEntity: Entity = {
		Entity(
			"test_key",
			Map(
				"nested_value:1.1" -> List("test_value:1.1"),
				"nested_value:1.2" -> List("test_value:1.2.1", "test_value:1.2.2")
			)
		)
	}

	def testEntities: List[Entity] = {
		List (
			Entity("entity1"),
			Entity("entity2"),
			Entity("entity3"),
			Entity("entity4"),
			Entity(root_value = null)
		)
	}

	def normalizationMapping: Map[String, List[String]] = {
		Map(
			"rootKey" -> List("root_value"),
			"nestedKey1" -> List("nested_value:1.1", "nested_value:1.2", "nested_value:1.3"),
			"nestedKey2" -> List("nested_value:2.1")
		)
	}

	def propertyMapping: Map[String, List[String]] = {
		Map(
			"rootKey" -> List("test_key"),
			"nestedKey1" -> List("test_value:1.1", "test_value:1.2.1", "test_value:1.2.2")
		)
	}

	def strategyMapping: Map[String, List[String]] = {
		Map(
			"cars" -> List("45", "29"),
			"music" -> List("1337"),
			"games" -> List("1996")
		)
	}

	def categoryMapping: Map[String, List[String]] = {
		Map(
			"Category 1" -> List("value1.1", "value1.2"),
			"Category 2" -> List("value2.1"),
			"Category 3" -> List("value3.1", "value3.2")
		)
	}

	def subjects: List[Subject] = {
		List(
			Subject(master = null, datasource = datasource, name = Some("Volkswagen"), properties = Map("geo_city" -> List("Berlin", "Hamburg"), "geo_coords" -> List("52; 11"), "gen_income" -> List("1234"))),
			Subject(master = null, datasource = datasource, name = Some("Volkswagen AG"), properties = Map("geo_city" -> List("Berlin", "New York"), "gen_income" -> List("12"))),
			Subject(master = null, datasource = datasource, name = Some("Audi GmbH"), properties = Map("geo_city" -> List("Berlin"), "geo_coords" -> List("52; 13"), "gen_income" -> List("33"))),
			Subject(master = null, datasource = datasource, name = Some("Audy GmbH"), properties = Map("geo_city" -> List("New York", "Hamburg"), "geo_coords" -> List("53; 14"), "gen_income" -> List("600"))),
			Subject(master = null, datasource = datasource, name = Some("Porsche"), properties = Map("geo_coords" -> List("52; 13"), "gen_sectors" -> List("cars", "music", "games"))),
			Subject(master = null, datasource = datasource, name = Some("Ferrari"), properties = Map("geo_coords" -> List("53; 14"), "gen_sectors" -> List("games", "cars", "music"))))
	}

	def version(sc: SparkContext): Version = {
		Version("SomeTestApp", Nil, sc, false, None)
	}

	def translationEntities: List[Entity] = {
		List(
			Entity("e 1", Map("key 1" -> List("value 1", "value 2"))),
			Entity("e 2", Map("key 2" -> List("value 1"),"key 3" -> List("value 2"))),
			Entity("e 3", Map("key 1" -> Nil)),
			Entity("e 4")
		)
	}

	def translatedSubjects: List[Subject] = {
		List(
			Subject(id = null, master = null, datasource = datasource, name = Option("e 1"), properties = Map("key 1" -> List("value 1", "value 2"))),
			Subject(id = null, master = null, datasource = datasource, name = Option("e 2"), properties = Map("key 2" -> List("value 1"),"key 3" -> List("value 2"))),
			Subject(id = null, master = null, datasource = datasource, name = Option("e 3"), properties = Map("key 1" -> Nil)),
			Subject(id = null, master = null, datasource = datasource, name = Option("e 4"))
		)
	}

	def companyNames: Map[String, Option[String]] = {
		Map(
			"Audisport AG" -> Option("AG"),
			"Audiobuch Verlag oHG" -> Option("oHG"),
			"Audax GmbH" -> Option("GmbH"),
			"Audio Active" -> None,
			"" -> None
		)
	}

	def output: List[Subject] = {
		List(
			Subject(master = null, datasource = datasource, name = Option("entity1")),
			Subject(master = null, datasource = datasource, name = Option("entity2")),
			Subject(master = null, datasource = datasource, name = Option("entity3")),
			Subject(master = null, datasource = datasource, name = Option("entity4")),
			Subject(master = null, datasource = datasource, name = None)
		)
	}

	def exportSubjects: List[Subject] = {
		List(
			Subject(
				id = UUID.fromString("4fbc0340-4862-431f-9c28-a508234b8130"),
				master = UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d"),
				datasource = datasource,
				name = Option("Name 1"),
				aliases = List("Alias 1", "Alias 1.1\""),
				category = None,
				properties = Map(),
				relations = Map(
					UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d") -> Map("owned by" -> "")
				)
			),
			Subject(
				id = UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d"),
				master = UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d"),
				datasource = datasource,
				name = Option("Name 2"),
				aliases = List("Alias 2"),
				category = Option("Category 2"),
				properties = Map("test_prop" -> List("value 1", "value 2")),
				relations = Map(
					UUID.fromString("4fbc0340-4862-431f-9c28-a508234b8130") -> Map("owns" -> "", "parent organization" -> ""),
					UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d") -> Map("owned by" -> "")
				)
			)
		)
	}

	def exportedNodeCSV: List[String] = {
		List(
			"\"4fbc0340-4862-431f-9c28-a508234b8130\",\"Name 1\",\"Alias 1;Alias 1.1\\\"\",\"\",\"\"",
			"\"831f2c54-33d5-43fc-a515-d871946a655d\",\"Name 2\",\"Alias 2\",\"Category 2\",\"\""
		)
	}

	def exportedMasterNodeCSV: List[String] = {
		List(
			"\"831f2c54-33d5-43fc-a515-d871946a655d\",\"Name 2\",\"Alias 2\",\"Category 2\",\"\""
		)
	}

	def exportedEdgeCSV: List[String] = {
		List(
			"4fbc0340-4862-431f-9c28-a508234b8130,831f2c54-33d5-43fc-a515-d871946a655d,owned by",
			"831f2c54-33d5-43fc-a515-d871946a655d,4fbc0340-4862-431f-9c28-a508234b8130,owns",
			"831f2c54-33d5-43fc-a515-d871946a655d,831f2c54-33d5-43fc-a515-d871946a655d,owned by"
		)
	}

	def exportedMasterEdgeCSV: List[String] = {
		List(
			"831f2c54-33d5-43fc-a515-d871946a655d,4fbc0340-4862-431f-9c28-a508234b8130,owns",
			"831f2c54-33d5-43fc-a515-d871946a655d,831f2c54-33d5-43fc-a515-d871946a655d,owned by"
		)
	}

	def versionQueries(): List[(UUID, List[Version])] = {
		List(
			(UUID.fromString("2195bc70-f6ba-11e6-aa16-63ef39f49c5d"), List(
				Version(UUID.fromString("2195bc70-f6ba-11e6-aa16-63ef39f49c5d"), "test"),
				Version(UUID.fromString("224e1a50-13e2-11e7-9a30-7384674b582f"), "test"))),
			(UUID.fromString("224e1a50-13e2-11e7-9a30-7384674b582f"), List(
				Version(UUID.fromString("2195bc70-f6ba-11e6-aa16-63ef39f49c5d"), "test"),
				Version(UUID.fromString("f44df8b0-2425-11e7-aec2-2d07f82c7921"), "test"))),
			(UUID.fromString("7b410340-243e-11e7-937a-ad9adce5e136"), List(
				Version(UUID.fromString("2195bc70-f6ba-11e6-aa16-63ef39f49c5d"), "test"),
				Version(UUID.fromString("224e1a50-13e2-11e7-9a30-7384674b582f"), "test"),
				Version(UUID.fromString("f44df8b0-2425-11e7-aec2-2d07f82c7921"), "test"))),
			(UUID.fromString("2195bc70-f6ba-11e6-aa16-63ef39f49c5d"), List(
				Version(UUID.fromString("224e1a50-13e2-11e7-9a30-7384674b582f"), "test"),
				Version(UUID.fromString("f44df8b0-2425-11e7-aec2-2d07f82c7921"), "test"),
				Version(UUID.fromString("7b410340-243e-11e7-937a-ad9adce5e136"), "test")))
		)
	}

	def versionQueryResults(): List[Option[UUID]] = {
		List(
			Option(UUID.fromString("2195bc70-f6ba-11e6-aa16-63ef39f49c5d")),
			Option(UUID.fromString("2195bc70-f6ba-11e6-aa16-63ef39f49c5d")),
			Option(UUID.fromString("f44df8b0-2425-11e7-aec2-2d07f82c7921")),
			None
		)
	}
}
// scalastyle:on line.size.limit
// scalastyle:on method.length
