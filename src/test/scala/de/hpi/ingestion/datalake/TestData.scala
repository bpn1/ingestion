package de.hpi.ingestion.datalake

import de.hpi.ingestion.datalake.mock.Entity
import de.hpi.ingestion.datalake.models.{Subject, Version}
import org.apache.spark.SparkContext

// scalastyle:off line.size.limit
object TestData {
	def testEntity: Entity = Entity(
		"test_key",
		Map(
			"nested_value:1.1" -> List("test_value:1.1"),
			"nested_value:1.2" -> List("test_value:1.2.1", "test_value:1.2.2")
		)
	)

	def configMapping: Map[String, String] = Map(
		"outputKeyspace" -> "datalake",
		"outputTable" -> "subject_temp",
		"versionTable" -> "version"
	)

	def normalizationMapping: Map[String, List[String]] = Map(
		"rootKey" -> List("root_value"),
		"nestedKey1" -> List("nested_value:1.1", "nested_value:1.2", "nested_value:1.3"),
		"nestedKey2" -> List("nested_value:2.1")
	)

	def propertyMapping: Map[String, List[String]] = Map(
		"rootKey" -> List("test_key"),
		"nestedKey1" -> List("test_value:1.1", "test_value:1.2.1", "test_value:1.2.2")
	)

	def strategyMapping: Map[String, List[String]] = Map(
		"cars" -> List("45", "29"),
		"music" -> List("1337"),
		"games" -> List("1996")
	)

	def subjects: List[Subject] = {
		List(
			Subject(name = Some("Volkswagen"), properties = Map("geo_city" -> List("Berlin", "Hamburg"), "geo_coords" -> List("52","11"), "gen_income" -> List("1234"))),
			Subject(name = Some("Volkswagen AG"), properties = Map("geo_city" -> List("Berlin", "New York"), "gen_income" -> List("12"))),
			Subject(name = Some("Audi GmbH"), properties = Map("geo_city" -> List("Berlin"), "geo_coords" -> List("52","13"), "gen_income" -> List("33"))),
			Subject(name = Some("Audy GmbH"), properties = Map("geo_city" -> List("New York", "Hamburg"), "geo_coords" -> List("53","14"), "gen_income" -> List("600"))),
			Subject(name = Some("Porsche"), properties = Map("geo_coords" -> List("52","13"), "gen_sectors" -> List("cars", "music", "games"))),
			Subject(name = Some("Ferrari"), properties = Map("geo_coords" -> List("53","14"), "gen_sectors" -> List("games", "cars", "music"))))
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

}
// scalastyle:on line.size.limit
