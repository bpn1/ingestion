package de.hpi.ingestion.datamerge

import java.util.UUID

import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.models.{Candidate, Duplicates}

// scalastyle:off
object TestData {
	val idList = List.fill(20)(UUID.randomUUID()).sorted
	val stagingSource = "wikidata"
	val version = Version(program = "Merging Test")

	def subjects: List[Subject] = List(
		Subject(
			id = idList.head,
			master = idList.head,
			datasource = "master",
			name = Option("Firma A"),
			properties = Map(
				"id_implisense" -> List("1"),
				"geo_postal" -> List("10777")
			),
			relations = Map(
				idList(1) -> SubjectManager.masterRelation(1.0),
				idList(12) -> Map("country" -> "0.5", "county" -> "0.6")
			)
		),
		Subject(
			id = idList(1),
			master = idList.head,
			datasource = "implisense",
			name = Option("Firma A"),
			properties = Map(
				"id_implisense" -> List("1"),
				"geo_postal" -> List("10777")
			),
			relations = Map(
				idList.head -> SubjectManager.slaveRelation(1.0),
				idList(12) -> Map("country" -> "0.5", "county" -> "0.6")
			)
		),
		Subject(
			id = idList(2),
			master = idList(2),
			datasource = "master",
			name = Option("Firma B"),
			properties = Map(
				"id_implisense" -> List("2"),
				"gen_urls" -> List("http://curation.de"),
				"geo_city" -> List("Berlin"),
				"gen_legal_form" -> List("GmbH")
			),
			relations = Map(
				idList(3) -> SubjectManager.masterRelation(1.0),
				idList(4) -> SubjectManager.masterRelation(1.0),
				idList(13) -> Map("county" -> ""),
				idList(14) -> Map("city" -> "0.9"),
				idList(15) -> Map("successor" -> "01.01.2212")
			)
		),
		Subject(
			id = idList(3),
			master = idList(2),
			datasource = "human",
			name = Option("Firma B"),
			properties = Map(
				"gen_urls" -> List("http://curation.de"),
				"geo_city" -> List("Berlin")
			),
			relations = Map(
				idList(2) -> SubjectManager.slaveRelation(1.0),
				idList(13) -> Map("county" -> ""),
				idList(14) -> Map("city" -> "0.9")
			)
		),
		Subject(
			id = idList(4),
			master = idList(2),
			datasource = "implisense",
			name = Option("Firma B"),
			properties = Map(
				"id_implisense" -> List("2"),
				"gen_urls" -> List("http://curation.de", "http://nahverkehr.de"),
				"geo_city" -> List("Potsdam"),
				"gen_legal_form" -> List("GmbH"),
				"implisenseAttribute" -> List("not normalized")
			),
			relations = Map(
				idList(2) -> SubjectManager.slaveRelation(1.0),
				idList(15) -> Map("successor" -> "01.01.2212")
			)
		)
	)

	def staging: List[Subject] = List(
		Subject(
			id = idList(5),
			master = idList(5),
			datasource = stagingSource,
			name = Option("Firma A"),
			category = Option("Kategorie A"),
			properties = Map(
				"geo_postal" -> List("10888"),
				"geo_city" -> List("Berlin")
			),
			relations = Map(
				idList(7) -> Map("successor" -> "")
			)
		),
		Subject(
			id = idList(6),
			master = idList(6),
			datasource = stagingSource,
			name = Option("Firma AA"),
			aliases = List("AA"),
			category = Option("Kategorie AA"),
			properties = Map(
				"geo_city" -> List("Potsdam"),
				"geo_county" -> List("Landkreis Neu-Brandenburg")
			)
		),
		Subject(
			id = idList(7),
			master = idList(7),
			datasource = stagingSource,
			name = Option("Firma B GmbH & Co. KG"),
			properties = Map(
				"gen_legal_form" -> List("GmbH & Co. KG"),
				"gen_sectors" -> List("Sector B"),
				"wikidataAttribute" -> List("not normalized")
			)
		),
		Subject(
			id = idList(8),
			master = idList(8),
			datasource = stagingSource,
			name = Option("Firma C"),
			aliases = List("Firma Berlin C"),
			properties = Map(
				"geo_city" -> List("Berlin"),
				"wikidataAttribute" -> List("not normalized")
			),
			relations = Map(
				idList(9) -> Map("ownerOf" -> "")
			)
		),
		Subject(
			id = idList(9),
			master = idList(9),
			datasource = stagingSource,
			name = Option("Firma D"),
			relations = Map(
				idList(8) -> Map("subsidiary" -> "")
			)
		)
	)

	def duplicates: List[Duplicates] = List(
		Duplicates(
			subjects(1).id,
			subjects(1).name,
			stagingSource,
			List(
				Candidate(staging.head.id, staging.head.name, 1.0),
				Candidate(staging(1).id, staging(1).name, 0.8)
			)
		),
		Duplicates(
			subjects(3).id,
			subjects(3).name,
			stagingSource,
			List(
				Candidate(staging(2).id, staging(2).name, 0.9)
			)
		)
	)

	def mergedSubjects: List[Subject] = List(
		Subject(
			id = idList.head,
			master = idList.head,
			datasource = "master",
			name = Option("Firma A"),
			aliases = List("Firma AA", "AA"),
			category = Option("Kategorie A"),
			properties = Map(
				"id_implisense" -> List("1"),
				"geo_postal" -> List("10777"),
				"geo_city" -> List("Potsdam", "Berlin"),
				"geo_county" -> List("Landkreis Neu-Brandenburg")
			),
			relations = Map(
				idList(1) -> SubjectManager.masterRelation(1.0),
				idList(5) -> SubjectManager.masterRelation(1.0),
				idList(6) -> SubjectManager.masterRelation(0.8),
				idList(12) -> Map("country" -> "0.5", "county" -> "0.6"),
				idList(7) -> Map("successor" -> "")
			)
		),
		Subject(
			id = idList(1),
			master = idList.head,
			datasource = "implisense",
			name = Option("Firma A"),
			properties = Map(
				"id_implisense" -> List("1"),
				"geo_postal" -> List("10777")
			),
			relations = Map(
				idList.head -> SubjectManager.slaveRelation(1.0),
				idList(12) -> Map("country" -> "0.5", "county" -> "0.6"),
				idList(5) -> SubjectManager.isDuplicateRelation(1.0),
				idList(6) -> SubjectManager.isDuplicateRelation(0.8)
			)
		),
		Subject(
			id = idList(2),
			master = idList(2),
			datasource = "master",
			name = Option("Firma B"),
			aliases = List("Firma B GmbH & Co. KG"),
			properties = Map(
				"id_implisense" -> List("2"),
				"gen_urls" -> List("http://curation.de"),
				"geo_city" -> List("Berlin"),
				"gen_legal_form" -> List("GmbH"),
				"gen_sectors" -> List("Sector B")
			),
			relations = Map(
				idList(3) -> SubjectManager.masterRelation(1.0),
				idList(7) -> SubjectManager.masterRelation(0.9),
				idList(13) -> Map("county" -> ""),
				idList(14) -> Map("city" -> "0.9"),
				idList(15) -> Map("successor" -> "01.01.2212")
			)
		),
		Subject(
			id = idList(3),
			master = idList(2),
			datasource = "human",
			name = Option("Firma B"),
			properties = Map(
				"gen_urls" -> List("http://curation.de"),
				"geo_city" -> List("Berlin")
			),
			relations = Map(
				idList(2) -> SubjectManager.slaveRelation(1.0),
				idList(13) -> Map("county" -> ""),
				idList(14) -> Map("city" -> "0.9"),
				idList(7) -> SubjectManager.isDuplicateRelation(0.9)
			)
		),
		Subject(
			id = idList(4),
			master = idList(2),
			datasource = "implisense",
			name = Option("Firma B"),
			properties = Map(
				"id_implisense" -> List("2"),
				"gen_urls" -> List("http://curation.de", "http://nahverkehr.de"),
				"geo_city" -> List("Potsdam"),
				"gen_legal_form" -> List("GmbH"),
				"implisenseAttribute" -> List("not normalized")
			),
			relations = Map(
				idList(2) -> SubjectManager.slaveRelation(1.0),
				idList(15) -> Map("successor" -> "01.01.2212")
			)
		),
		Subject(
			id = idList(5),
			master = idList.head,
			datasource = stagingSource,
			name = Option("Firma A"),
			category = Option("Kategorie A"),
			properties = Map(
				"geo_postal" -> List("10888"),
				"geo_city" -> List("Berlin")
			),
			relations = Map(
				idList(7) -> Map("successor" -> ""),
				idList.head -> SubjectManager.slaveRelation(1.0),
				idList(1) -> SubjectManager.isDuplicateRelation(1.0)
			)
		),
		Subject(
			id = idList(6),
			master = idList.head,
			datasource = stagingSource,
			name = Option("Firma AA"),
			aliases = List("AA"),
			category = Option("Kategorie AA"),
			properties = Map(
				"geo_city" -> List("Potsdam"),
				"geo_county" -> List("Landkreis Neu-Brandenburg")
			),
			relations = Map(
				idList.head -> SubjectManager.slaveRelation(0.8),
				idList(1) -> SubjectManager.isDuplicateRelation(0.8)
			)
		),
		Subject(
			id = idList(7),
			master = idList(2),
			datasource = stagingSource,
			name = Option("Firma B GmbH & Co. KG"),
			properties = Map(
				"gen_legal_form" -> List("GmbH & Co. KG"),
				"gen_sectors" -> List("Sector B"),
				"wikidataAttribute" -> List("not normalized")
			),
			relations = Map(
				idList(2) -> SubjectManager.slaveRelation(0.9),
				idList(3) -> SubjectManager.isDuplicateRelation(0.9)
			)
		),
		Subject(
			id = idList(10),
			master = idList(10),
			datasource = "master",
			name = Option("Firma C"),
			aliases = List("Firma Berlin C"),
			properties = Map("geo_city" -> List("Berlin")),
			relations = Map(
				idList(8) -> SubjectManager.masterRelation(1.0),
				idList(9) -> Map("ownerOf" -> "")
			)
		),
		Subject(
			id = idList(8),
			master = idList(10),
			datasource = stagingSource,
			name = Option("Firma C"),
			aliases = List("Firma Berlin C"),
			properties = Map(
				"geo_city" -> List("Berlin"),
				"wikidataAttribute" -> List("not normalized")
			),
			relations = Map(
				idList(10) -> SubjectManager.slaveRelation(1.0),
				idList(9) -> Map("ownerOf" -> "")
			)
		),
		Subject(
			id = idList(11),
			master = idList(11),
			datasource = "master",
			name = Option("Firma D"),
			relations = Map(
				idList(9) -> SubjectManager.masterRelation(1.0),
				idList(8) -> Map("subsidiary" -> "")
			)
		),
		Subject(
			id = idList(9),
			master = idList(11),
			datasource = stagingSource,
			name = Option("Firma D"),
			relations = Map(
				idList(11) -> SubjectManager.slaveRelation(1.0),
				idList(8) -> Map("subsidiary" -> "")
			)
		)
	)

	def connectedMasters: List[Subject] = List(
		Subject(
			id = idList.head,
			master = idList.head,
			datasource = "master",
			name = Option("Firma A"),
			aliases = List("Firma AA", "AA"),
			category = Option("Kategorie A"),
			properties = Map(
				"id_implisense" -> List("1"),
				"geo_postal" -> List("10777"),
				"geo_city" -> List("Potsdam", "Berlin"),
				"geo_county" -> List("Landkreis Neu-Brandenburg")
			),
			relations = Map(
				idList(1) -> SubjectManager.masterRelation(1.0),
				idList(5) -> SubjectManager.masterRelation(1.0),
				idList(6) -> SubjectManager.masterRelation(0.8),
				idList(12) -> Map("country" -> "0.5", "county" -> "0.6"),
				idList(2) -> Map("successor" -> "")
			)
		),
		Subject(
			id = idList(10),
			master = idList(10),
			datasource = "master",
			name = Option("Firma C"),
			aliases = List("Firma Berlin C"),
			properties = Map("geo_city" -> List("Berlin")),
			relations = Map(
				idList(8) -> SubjectManager.masterRelation(1.0),
				idList(11) -> Map("ownerOf" -> "")
			)
		),
		Subject(
			id = idList(11),
			master = idList(11),
			datasource = "master",
			name = Option("Firma D"),
			relations = Map(
				idList(9) -> SubjectManager.masterRelation(1.0),
				idList(10) -> Map("subsidiary" -> "")
			)
		)
	)
}
// scalastyle:on
