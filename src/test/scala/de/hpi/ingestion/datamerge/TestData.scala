package de.hpi.ingestion.datamerge

import de.hpi.ingestion.datalake.models.{Subject, Version}
import org.apache.spark.SparkContext

object TestData {
	// scalastyle:off line.size.limit
	def testSubjects: List[Subject] = List(
		Subject(name = Some("Volkswagen"), properties = Map("geo_city" -> List("Berlin", "Hamburg"), "geo_coords" -> List("52","11"), "gen_income" -> List("1234"))),
		Subject(name = Some("Volkswagen AG"), properties = Map("geo_city" -> List("Berlin", "New York"), "gen_income" -> List("12"))),
		Subject(name = Some("Audi GmbH"), properties = Map("geo_city" -> List("Berlin"), "geo_coords" -> List("52","13"), "gen_income" -> List("33"))),
		Subject(name = Some("Audy GmbH"), properties = Map("geo_city" -> List("New York", "Hamburg"), "geo_coords" -> List("53","14"), "gen_income" -> List("600"))),
		Subject(name = Some("Porsche"), properties = Map("geo_coords" -> List("52","13"), "gen_sectors" -> List("cars", "music", "games"))),
		Subject(name = Some("Ferrari"), properties = Map("geo_coords" -> List("53","14"), "gen_sectors" -> List("games", "cars", "music")))
	)

	def testVersion(sc: SparkContext): Version = Version("SomeTestApp", Nil, sc)
	// scalastyle:on line.size.limit
}
