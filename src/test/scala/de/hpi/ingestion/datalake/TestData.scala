package de.hpi.ingestion.datalake

import java.util.UUID
import de.hpi.ingestion.datalake.mock.Entity
import de.hpi.ingestion.datalake.models.{Subject, Version}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

// scalastyle:off line.size.limit
object TestData {

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
			Subject(name = Some("Volkswagen"), properties = Map("geo_city" -> List("Berlin", "Hamburg"), "geo_coords" -> List("52; 11"), "gen_income" -> List("1234"))),
			Subject(name = Some("Volkswagen AG"), properties = Map("geo_city" -> List("Berlin", "New York"), "gen_income" -> List("12"))),
			Subject(name = Some("Audi GmbH"), properties = Map("geo_city" -> List("Berlin"), "geo_coords" -> List("52; 13"), "gen_income" -> List("33"))),
			Subject(name = Some("Audy GmbH"), properties = Map("geo_city" -> List("New York", "Hamburg"), "geo_coords" -> List("53; 14"), "gen_income" -> List("600"))),
			Subject(name = Some("Porsche"), properties = Map("geo_coords" -> List("52; 13"), "gen_sectors" -> List("cars", "music", "games"))),
			Subject(name = Some("Ferrari"), properties = Map("geo_coords" -> List("53; 14"), "gen_sectors" -> List("games", "cars", "music"))))
	}

	def version(sc: SparkContext): Version = {
		Version("SomeTestApp", Nil, sc, false)
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
			Subject(id = null, name = Option("e 1"), properties = Map("key 1" -> List("value 1", "value 2"))),
			Subject(id = null, name = Option("e 2"), properties = Map("key 2" -> List("value 1"),"key 3" -> List("value 2"))),
			Subject(id = null, name = Option("e 3"), properties = Map("key 1" -> Nil)),
			Subject(id = null, name = Option("e 4"))
		)
	}

	def companyNames: Map[String, List[String]] = {
		Map(
			"Audisport AG" -> List("AG"),
			"Audiobuch Verlag oHG" -> List("oHG"),
			"Audax GmbH" -> List("GmbH"),
			"Audio Active" -> Nil,
			"" -> Nil
		)
	}

	def input(sc: SparkContext): List[RDD[Any]] = {
		List(sc.parallelize(testEntities))
	}

	def output: List[Subject] = {
		List(
			Subject(name = Option("entity1")),
			Subject(name = Option("entity2")),
			Subject(name = Option("entity3")),
			Subject(name = Option("entity4")),
			Subject(name = None)
		)
	}

	def exportSubjects: List[Subject] = {
		List(
			Subject(
				id = UUID.fromString("4fbc0340-4862-431f-9c28-a508234b8130"),
				name = Option("Name 1"),
				aliases = List("Alias 1", "Alias 1.1\""),
				category = None,
				properties = Map(),
				relations = Map(
					UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d") -> Map("relation" -> "test 1", "type" -> "test"),
					UUID.fromString("4fbc0340-4862-431f-9c28-a508234b8130") -> Map("type" -> "reflexive")
				)
			),
			Subject(
				id = UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d"),
				name = Option("Name 2"),
				aliases = List("Alias 2"),
				category = Option("Category 2"),
				properties = Map("test_prop" -> List("value 1", "value 2")),
				relations = Map(
					UUID.fromString("4fbc0340-4862-431f-9c28-a508234b8130") -> Map("relation" -> "test 2")
				)
			)
		)
	}

	def exportedNodeCSV: List[String] = {
		List(
			"\"4fbc0340-4862-431f-9c28-a508234b8130\",\"Name 1\",\"Alias 1;Alias 1.1\\\"\",\"\"",
			"\"831f2c54-33d5-43fc-a515-d871946a655d\",\"Name 2\",\"Alias 2\",\"Category 2\""
		)
	}

	def exportedEdgeCSV: List[String] = {
		List(
			"4fbc0340-4862-431f-9c28-a508234b8130,831f2c54-33d5-43fc-a515-d871946a655d,test",
			"4fbc0340-4862-431f-9c28-a508234b8130,4fbc0340-4862-431f-9c28-a508234b8130,reflexive",
			"831f2c54-33d5-43fc-a515-d871946a655d,4fbc0340-4862-431f-9c28-a508234b8130,"
		)
	}
}
// scalastyle:on line.size.limit
