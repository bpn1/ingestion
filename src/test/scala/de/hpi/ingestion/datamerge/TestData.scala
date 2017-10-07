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

package de.hpi.ingestion.datamerge

import java.util.UUID

import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.models.{Candidate, Duplicates}

// scalastyle:off line.size.limit
// scalastyle:off method.length
// scalastyle:off file.size.limit
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

	def inputSubjects(): List[Subject] = {
		List(
			Subject(
				id = idList.head,
				master = idList.head,
				datasource = "master",
				relations = Map(
					idList(4) -> Map("t1" -> "impli", "t3" -> "impli"),
					idList(5) -> Map("t2" -> "wiki", "t3" -> "wiki")
				)
			),
			Subject(
				id = idList(1),
				master = idList(1),
				datasource = "master",
				relations = Map(
					idList(2) -> Map("t1" -> "impli", "t3" -> "impli"),
					idList(3) -> Map("t2" -> "wiki", "t4" -> "wiki")
				)
			),
			Subject(
				id = idList(2),
				master = idList.head,
				datasource = "implisense"
			),
			Subject(
				id = idList(3),
				master = idList.head,
				datasource = "wikidata"
			),
			Subject(
				id = idList(4),
				master = idList(1),
				datasource = "implisense"
			),
			Subject(
				id = idList(5),
				master = idList(1),
				datasource = "wikidata"
			)
		)
	}

	def mergedMasters(): List[Subject] = {
		List(
			Subject(
				id = idList.head,
				master = idList.head,
				datasource = "master",
				relations = Map(
					idList(1) -> Map("t1" -> "impli", "t3" -> "impli", "t2" -> "wiki")
				)
			),
			Subject(
				id = idList(1),
				master = idList(1),
				datasource = "master",
				relations = Map(
					idList.head -> Map("t1" -> "impli", "t3" -> "impli", "t2" -> "wiki", "t4" -> "wiki")
				)
			)
		)
	}

	def outdatedMasters(): List[Subject] = {
		List(
			Subject(
				id = idList.head,
				master = idList.head,
				datasource = "master",
				name = Option("Firma A"),
				properties = Map(
					"id_implisense" -> List("1"),
					"geo_postal" -> List("10777"),
					"geo_street" -> List("Strasse 1"),
					"geo_country" -> List("DE")
				),
				relations = Map(
					idList(1) -> SubjectManager.masterRelation(1.0),
					idList(9) -> Map("country" -> "0.4", "county" -> "0.3"),
					idList(10) -> Map("city" -> "0.1", "country" -> "1.0"),
					idList(12) -> Map("country" -> "0.5", "county" -> "0.6")
				)
			),
			Subject(
				id = idList(1),
				master = idList.head,
				datasource = "implisense",
				name = Option("Firma B"),
				properties = Map(
					"id_implisense" -> List("1"),
					"geo_postal" -> List("10777"),
					"geo_street" -> List("Strasse 2"),
					"geo_city" -> List("Berlin")
				),
				relations = Map(
					idList.head -> SubjectManager.slaveRelation(1.0),
					idList(10) -> Map("street" -> "0.4", "country" -> "0.3"),
					idList(11) -> Map("owns" -> "0.1", "follows" -> "0.2"),
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
					"gen_urls" -> List("http://nahverkehr.de"),
					"geo_city" -> List("Potsdam"),
					"gen_legal_form" -> List("GmbH")
				),
				relations = Map(
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
				name = Option("Firma C"),
				aliases = List("Firma D"),
				properties = Map(
					"gen_urls" -> List("http://curation.com"),
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
				name = Option("Firma E"),
				properties = Map(
					"id_implisense" -> List("2"),
					"gen_urls" -> List("http://nahverkehr.de"),
					"geo_city" -> List("Potsdam"),
					"gen_legal_form" -> List("GmbH & Co. KG"),
					"implisenseAttribute" -> List("not normalized")
				),
				relations = Map(
					idList(2) -> SubjectManager.slaveRelation(1.0),
					idList(15) -> Map("successor" -> "01.01.2212")
				)
			)
		)
	}

	def updatedMasters(): List[Subject] = {
		List(
			Subject(
				id = idList.head,
				master = idList.head,
				datasource = "master",
				name = Option("Firma B"),
				properties = Map(
					"id_implisense" -> List("1"),
					"geo_postal" -> List("10777"),
					"geo_street" -> List("Strasse 2"),
					"geo_city" -> List("Berlin")
				),
				relations = Map(
					idList(1) -> SubjectManager.masterRelation(1.0),
					idList(10) -> Map("street" -> "0.4", "country" -> "0.3"),
					idList(11) -> Map("owns" -> "0.1", "follows" -> "0.2"),
					idList(12) -> Map("country" -> "0.5", "county" -> "0.6")
				)
			),
			Subject(
				id = idList(2),
				master = idList(2),
				datasource = "master",
				name = Option("Firma C"),
				aliases = List("Firma E", "Firma D"),
				properties = Map(
					"id_implisense" -> List("2"),
					"gen_urls" -> List("http://curation.com"),
					"geo_city" -> List("Berlin"),
					"gen_legal_form" -> List("GmbH & Co. KG")
				),
				relations = Map(
					idList(3) -> SubjectManager.masterRelation(1.0),
					idList(4) -> SubjectManager.masterRelation(1.0),
					idList(13) -> Map("county" -> ""),
					idList(14) -> Map("city" -> "0.9"),
					idList(15) -> Map("successor" -> "01.01.2212")
				)
			)
		)
	}

	def subjectsToUpdate(): List[Subject] = {
		List(
			Subject(
				id = UUID.fromString("2195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
				master = UUID.fromString("3195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
				datasource = "wikidata",
				name = Option("Name 1"),
				properties = Map(
					"id_implisense" -> List("1"),
					"geo_city" -> List("Potsdam"),
					"geo_county" -> List("Tempelhof")
				),
				category = Option("organization"),
				relations = Map(
					UUID.fromString("4195bc70-f6ba-11e6-aa16-63ef39f49c5d") -> Map("owned by" -> ""),
					UUID.fromString("3195bc70-f6ba-11e6-aa16-63ef39f49c5d") -> Map("follows" -> "")
				)
			)
		)
	}

	def updateSubjects(): List[Subject] = {
		List(
			Subject(
				id = UUID.fromString("e44df8b0-2425-11e7-aec2-2d07f82c7921"),
				master = UUID.fromString("e44df8b0-2425-11e7-aec2-2d07f82c7921"),
				datasource = "wikidata",
				name = Option("Name 1"),
				properties = Map(
					"id_implisense" -> List("1"),
					"geo_city" -> List("Berlin", "München"),
					"geo_country" -> List("DE")
				),
				aliases = List("Name 2", "Name 3"),
				category = Option("business"),
				relations = Map(
					UUID.fromString("4195bc70-f6ba-11e6-aa16-63ef39f49c5d") -> Map("owns" -> ""),
					UUID.fromString("5195bc70-f6ba-11e6-aa16-63ef39f49c5d") -> Map("owned by" -> "")
				)
			)
		)
	}

	def updatedSubjects(): List[Subject] = {
		List(
			Subject(
				id = UUID.fromString("2195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
				master = UUID.fromString("3195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
				datasource = "wikidata",
				name = Option("Name 1"),
				properties = Map(
					"id_implisense" -> List("1"),
					"geo_city" -> List("Berlin", "München"),
					"geo_country" -> List("DE")
				),
				category = Option("business"),
				aliases = List("Name 2", "Name 3"),
				relations = Map(
					UUID.fromString("4195bc70-f6ba-11e6-aa16-63ef39f49c5d") -> Map("owns" -> ""),
					UUID.fromString("5195bc70-f6ba-11e6-aa16-63ef39f49c5d") -> Map("owned by" -> "")
				)
			)
		)
	}

	def oldSubjects(): List[Subject] = {
		List(
			Subject(
				id = UUID.fromString("2195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
				master = UUID.fromString("3195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
				datasource = "wikidata",
				name = Option("Name 1"),
				properties = Map(
					"id_implisense" -> List("1"),
					"geo_city" -> List("Potsdam"),
					"geo_county" -> List("Tempelhof")
				),
				category = Option("organization"),
				relations = Map(
					UUID.fromString("4195bc70-f6ba-11e6-aa16-63ef39f49c5d") -> Map("owned by" -> ""),
					UUID.fromString("3195bc70-f6ba-11e6-aa16-63ef39f49c5d") -> Map("follows" -> "")
				)
			),
			Subject(
				id = UUID.fromString("3195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
				master = UUID.fromString("3195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
				datasource = "master",
				name = Option("Name 1"),
				properties = Map(
					"id_implisense" -> List("1"),
					"geo_city" -> List("Potsdam"),
					"geo_county" -> List("Tempelhof")
				),
				category = Option("organization"),
				relations = Map(
					UUID.fromString("4195bc70-f6ba-11e6-aa16-63ef39f49c5d") -> Map("owned by" -> ""),
					UUID.fromString("3195bc70-f6ba-11e6-aa16-63ef39f49c5d") -> Map("follows" -> "")
				)
			),
			Subject(
				id = UUID.fromString("2195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
				master = UUID.fromString("1195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
				datasource = "wikidata",
				name = Option("Name 1"),
				properties = Map(
					"id_implisense" -> List("2"),
					"geo_city" -> List("Potsdam"),
					"geo_county" -> List("Tempelhof")
				),
				category = Option("organization")
			),
			Subject(
				id = UUID.fromString("1195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
				master = UUID.fromString("1195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
				datasource = "master",
				name = Option("Name 1"),
				properties = Map(
					"id_implisense" -> List("2"),
					"geo_city" -> List("Potsdam"),
					"geo_county" -> List("Tempelhof")
				),
				category = Option("organization")
			)
		)
	}

	def newSubjects(): List[Subject] = {
		List(
			Subject(
				id = UUID.fromString("e44df8b0-2425-11e7-aec2-2d07f82c7921"),
				master = UUID.fromString("e44df8b0-2425-11e7-aec2-2d07f82c7921"),
				datasource = "wikidata",
				name = Option("Name 1"),
				properties = Map(
					"id_implisense" -> List("1"),
					"geo_city" -> List("Berlin", "München"),
					"geo_country" -> List("DE")
				),
				aliases = List("Name 2", "Name 3"),
				category = Option("business"),
				relations = Map(
					UUID.fromString("4195bc70-f6ba-11e6-aa16-63ef39f49c5d") -> Map("owns" -> ""),
					UUID.fromString("5195bc70-f6ba-11e6-aa16-63ef39f49c5d") -> Map("owned by" -> "")
				)
			),
			Subject(
				id = UUID.fromString("f44df8b0-2425-11e7-aec2-2d07f82c7921"),
				master = UUID.fromString("f44df8b0-2425-11e7-aec2-2d07f82c7921"),
				datasource = "wikidata",
				name = Option("Name 2"),
				properties = Map("id_implisense" -> List("3")),
				aliases = List("Name 3"),
				category = Option("business")
			)
		)
	}

	def updatedAndNewSubjects(): List[Subject] = {
		List(
			Subject(
				id = UUID.fromString("2195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
				master = UUID.fromString("3195bc70-f6ba-11e6-aa16-63ef39f49c5d"),
				datasource = "wikidata",
				name = Option("Name 1"),
				properties = Map(
					"id_implisense" -> List("1"),
					"geo_city" -> List("Berlin", "München"),
					"geo_country" -> List("DE")
				),
				category = Option("business"),
				aliases = List("Name 2", "Name 3"),
				relations = Map(
					UUID.fromString("4195bc70-f6ba-11e6-aa16-63ef39f49c5d") -> Map("owns" -> ""),
					UUID.fromString("5195bc70-f6ba-11e6-aa16-63ef39f49c5d") -> Map("owned by" -> "")
				)
			),
			Subject(
				id = UUID.fromString("f44df8b0-2425-11e7-aec2-2d07f82c7921"),
				master = UUID.fromString("d44df8b0-2425-11e7-aec2-2d07f82c7921"),
				datasource = "wikidata",
				name = Option("Name 2"),
				properties = Map("id_implisense" -> List("3")),
				aliases = List("Name 3"),
				category = Option("business"),
				relations = Map(UUID.fromString("d44df8b0-2425-11e7-aec2-2d07f82c7921") -> Map("slave" -> "1.0"))
			),
			Subject(
				id = UUID.fromString("d44df8b0-2425-11e7-aec2-2d07f82c7921"),
				master = UUID.fromString("d44df8b0-2425-11e7-aec2-2d07f82c7921"),
				datasource = "master",
				relations = Map(UUID.fromString("f44df8b0-2425-11e7-aec2-2d07f82c7921") -> Map("master" -> "1.0"))
			)
		)
	}
}
// scalastyle:on method.length
// scalastyle:on line.size.limit
// scalastyle:on file.size.limit
