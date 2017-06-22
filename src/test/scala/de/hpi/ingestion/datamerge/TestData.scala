package de.hpi.ingestion.datamerge

import java.util.UUID

import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.datamerge.TestData.idList
import de.hpi.ingestion.deduplication.models.{Candidate, Duplicates}

// scalastyle:off
object TestData {
	val masterId = UUID.randomUUID()
	val idList = List.fill(20)(UUID.randomUUID())
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
				idList(10) -> Map("country" -> "0.5", "county" -> "0.6")
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
				idList(10) -> Map("country" -> "0.5", "county" -> "0.6")
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
				idList(11) -> Map("county" -> ""),
				idList(12) -> Map("city" -> "0.9"),
				idList(13) -> Map("successor" -> "01.01.2212")
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
				idList(11) -> Map("county" -> ""),
				idList(12) -> Map("city" -> "0.9")
			)
		),
		Subject(
			id = idList(4),
			master = idList(2),
			datasource = "implisense",
			name = Option("Firma B"),
			properties = Map(
				"implisense" -> List("2"),
				"gen_urls" -> List("http://curation.de", "http://nahverkehr.de"),
				"geo_city" -> List("Potsdam"),
				"gen_legal_form" -> List("GmbH"),
				"implisenseAttribute" -> List("not normalized")
			),
			relations = Map(
				idList(2) -> SubjectManager.slaveRelation(1.0),
				idList(13) -> Map("successor" -> "01.01.2212")
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
				"geo_city" -> List("Berlin"),
				"geo_county" -> List("Landkreis Neu-Brandenburg")
			),
			relations = Map(
				idList(1) -> SubjectManager.masterRelation(1.0),
				idList(5) -> SubjectManager.masterRelation(1.0),
				idList(6) -> SubjectManager.masterRelation(0.8),
				idList(10) -> Map("country" -> "0.5", "county" -> "0.6"),
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
				idList(10) -> Map("country" -> "0.5", "county" -> "0.6"),
				idList(5) -> Map("duplicate" -> "1.0"),
				idList(6) -> Map("duplicate" -> "0.8")
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
				idList(11) -> Map("county" -> ""),
				idList(12) -> Map("city" -> "0.9"),
				idList(13) -> Map("successor" -> "01.01.2212")
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
				idList(11) -> Map("county" -> ""),
				idList(12) -> Map("city" -> "0.9"),
				idList(2) -> Map("duplicate" -> "0.9")
			)
		),
		Subject(
			id = idList(4),
			master = idList(2),
			datasource = "implisense",
			name = Option("Firma B"),
			properties = Map(
				"implisense" -> List("2"),
				"gen_urls" -> List("http://curation.de", "http://nahverkehr.de"),
				"geo_city" -> List("Potsdam"),
				"gen_legal_form" -> List("GmbH"),
				"implisenseAttribute" -> List("not normalized")
			),
			relations = Map(
				idList(2) -> SubjectManager.slaveRelation(1.0),
				idList(13) -> Map("successor" -> "01.01.2212")
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
				idList(1) -> Map("duplicate" -> "1.0")
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
				idList(1) -> Map("duplicate" -> "0.8")
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
				idList(3) -> Map("duplicate" -> "0.9")
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

	def variousDatasourceSubject: List[Subject] = {
		val subjects = List.fill(5)(Subject.empty())
		val datasources = "human" :: List.fill(2)("implisense") ::: List.fill(2)("wikidata")
		val conflictingProperties = List(
			Map("geo_postal" -> List("10777")),
			Map("geo_postal" -> List("10888"), "id_implisense" -> List("1")),
			Map("id_implisense" -> List("2"), "gen_urls" -> List("http://curation.de"), "gen_legal_form" -> List("GmbH")),
			Map("gen_urls" -> List("http://google.de", "http://google.com"), "geo_city" -> List("Berlin")),
			Map("geo_city" -> List("Potsdam"), "gen_legal_form" -> List("GmbH & Co. KG"), "unnormalized" -> List("test"))
		)
		val conflictingRelations = List(
			Map(idList.head -> Map("country" -> "0.8"), idList(1) -> Map("city" -> "")),
			Map(idList.head -> Map("country" -> "0.5", "county" -> "0.6")),
			Map(idList(2) -> Map("successor" -> "01.01.2212")),
			Map(idList.head -> Map("county" -> ""), idList(1) -> Map("city" -> "0.9")),
			Map[UUID, Map[String, String]]()
		)

		val attributes = (datasources, conflictingProperties, conflictingRelations).zipped.toList
		subjects
			.zip(attributes)
	    	.map { case (subject, (datasource, properties, relations)) => subject
				.copy(datasource = datasource, properties = properties, relations = relations)
			}
	}

	def mergedProperties: Map[String, List[String]] = {
		Map(
			"geo_postal" -> List("10777"),
			"id_implisense" -> List("1"),
			"gen_urls" -> List("http://curation.de"),
			"gen_legal_form" -> List("GmbH"),
			"geo_city" -> List("Berlin")
		)
	}

	def mergedRelations: Map[UUID, Map[String, String]] = {
		Map(
			idList.head -> Map("country" -> "0.8", "county" -> "0.6"),
			idList(1) -> Map("city" -> ""),
			idList(2) -> Map("successor" -> "01.01.2212")
		)
	}

	/*
	val subjectIds = List.fill(10)(UUID.randomUUID()).sorted
	val secondMasterIds = List.fill(2)(UUID.randomUUID()).sorted
	val masterName = Option("masterNode")
	val subjectNames = List.fill(10)("slaveNode").zipWithIndex.map { case (name, index) => Option(s"$name$index")}
	val secondMasterNames = List(Option("masterNode2.1"), Option("masterNode2.1"))
	val datasource = "testSource"

	def version(source: String, value: String*): Version = {
		Version(program = "MergeTest", datasources = List(source).filter(_.nonEmpty), value = value.toList)
	}

	def master: Subject = Subject.master(masterId)

	def masterSM(sub: Subject): SubjectManager = new SubjectManager(sub, version(""))

	def masterRelations(): Map[UUID, Map[String, String]] = {
		subjectIds.zipWithIndex.map { case (id, index) =>
			(id, Map("master" -> s"0.$index"))
		}.toMap
	}

	def mergedName(): Option[String] = Option("Name Human")
	def mergedCategory(): Option[String] = Option("Category 1")
	def mergedAliases(): List[String] = {
		List(
			"Name 1", "Name 9", "Name 2", "Name 5", "Name 6",
			"Alias 1", "Alias 2", "Alias Human", "Alias 6", "Alias 9"
		)
	}

	def reducedMergedAliases(): List[String] = {
		List("Name 1", "Name 2", "Name 5", "Alias 1", "Alias 2", "Alias Human", "Alias 6")
	}

	def mergedProperties(): Map[String, List[String]] = {
		Map(
			"gen_urls" -> List("val 3"),
			"geo_city" -> List("val 1", "val 2")
		)
	}

	def mergedRelations(): Map[UUID, Map[String, String]] = {
		Map(
			subjectIds(1) -> Map("type 1" -> "score"),
			subjectIds(2) -> Map("type 2" -> "score 2"),
			subjectIds(3) -> Map("type 3" -> "score 3", "type 4" -> "score 4")
		)
	}

	def mergedRelationsWithMaster(): Map[UUID, Map[String, String]] = {
		Map(
			subjectIds.head -> Map("master" -> "0.0"),
			subjectIds(1) -> Map("type 1" -> "score", "master" -> "0.1"),
			subjectIds(2) -> Map("type 2" -> "score 2", "master" -> "0.2"),
			subjectIds(3) -> Map("type 3" -> "score 3", "type 4" -> "score 4", "master" -> "0.3"),
			subjectIds(4) -> Map("master" -> "0.4"),
			subjectIds(5) -> Map("master" -> "0.5"),
			subjectIds(6) -> Map("master" -> "0.6"),
			subjectIds(7) -> Map("master" -> "0.7"),
			subjectIds(8) -> Map("master" -> "0.8"),
			subjectIds(9) -> Map("master" -> "0.9")
		)
	}

	def reducedMergesRelationsWithMaster(): Map[UUID, Map[String, String]] = {
		Map(
			subjectIds.head -> Map("master" -> "0.0"),
			subjectIds(1) -> Map("type 1" -> "score", "master" -> "0.1"),
			subjectIds(2) -> Map("type 2" -> "score 2", "master" -> "0.2"),
			subjectIds(3) -> Map("type 3" -> "score 3", "type 4" -> "score 4", "master" -> "0.3"),
			subjectIds(4) -> Map("master" -> "0.4"),
			subjectIds(7) -> Map("master" -> "1.0")
		)
	}

	def mergedMaster(): Subject = {
		Subject(
			id = masterId,
			master = masterId,
			datasource = null,
			name = mergedName(),
			category = mergedCategory(),
			aliases = mergedAliases(),
			properties = mergedProperties(),
			relations = mergedRelationsWithMaster()
		)
	}

	def duplicates(): List[Subject] = {
		List(
			Subject(
				id = subjectIds.head,
				master = masterId,
				datasource = null,
				name = Option("Name 1"),
				name_history = List(version("implisense", "Name 1")),
				category = Option("Category 1"),
				category_history = List(version("implisense", "Category 1")),
				aliases = List("Alias 1", "Alias 2"),
				aliases_history = List(version("implisense", "Alias 1", "Alias 2")),
				relations = Map(masterId -> Map("slave" -> "0.0"), subjectIds(1) -> Map("type 1" -> "score")),
				relations_history = Map(
					subjectIds(1) -> Map("type 1" -> List(version("implisense", "score"))),
					masterId -> Map("slave" -> List(version("merging", "0.0")))),
				properties = Map("gen_urls" -> List("val 1", "val 2")),
				properties_history = Map("gen_urls" -> List(version("implisense", "val 1", "val 2")))
			),
			Subject(
				id = subjectIds(1),
				master = masterId,
				datasource = null,
				name = Option("Name 2"),
				name_history = List(version("wikidata", "Name 2")),
				category = Option("Category 2"),
				category_history = List(version("dbpedia", "Category 2")),
				relations = Map(
					subjectIds(2) -> Map("type 2" -> "score 2"),
					masterId -> Map("slave" -> "0.1")),
				relations_history = Map(
					subjectIds(2) -> Map(
						"type 2" -> List(version("wikidata", "score 2")),
						"type 1" -> List(version("implisense", "score 2"), version("human"))),
					masterId -> Map("slave" -> List(version("merging", "0.1")))),
				properties = Map("geo_city" -> List("val 1", "val 2")),
				properties_history = Map("geo_city" -> List(version("wikidata", "val 1", "val 2")))
			),
			Subject(
				id = subjectIds(2),
				master = masterId,
				datasource = null,
				name = Option("Name Human"),
				name_history = List(version("human", "Name Human")),
				category = Option("Category 3"),
				category_history = List(version("error", "Category 3")),
				aliases = List("Alias Human"),
				aliases_history = List(version("human", "Alias Human")),
				relations = Map(
					subjectIds(3) -> Map("type 3" -> "score 3"),
					masterId -> Map("slave" -> "0.2")),
				relations_history = Map(
					subjectIds(3) -> Map("type 3" -> List(version("dbpedia", "score 3"))),
					masterId -> Map("slave" -> List(version("merging", "0.2")))),
				properties = Map("gen_urls" -> List("val 3")),
				properties_history = Map("gen_urls" -> List(version("human", "val 3")))
			),
			Subject(
				id = subjectIds(3),
				master = masterId,
				category = None,
				category_history = List(version("implisense", "Category 4"), version("human")),
				aliases = List("Alias 6"),
				aliases_history = List(version("dbpedia", "Alias 6")),
				relations = Map(
					subjectIds(3) -> Map("type 3" -> "score 33"),
					masterId -> Map("slave" -> "0.3")),
				relations_history = Map(
					subjectIds(3) -> Map("type 3" -> List(version("wikidata", "score 33"))),
					masterId -> Map("slave" -> List(version("merging", "0.3")))),
				properties = Map("geo_city" -> List("val 1")),
				properties_history = Map("geo_city" -> List(version("wikidata", "val 1")))
			),
			Subject(
				id = subjectIds(4),
				master = masterId,
				name = Option("Name 5"),
				name_history = List(version("wikidata", "Name 5")),
				relations = Map(
					subjectIds(3) -> Map("type 4" -> "score 4"),
					masterId -> Map("slave" -> "0.4")),
				relations_history = Map(
					subjectIds(3) -> Map("type 4" -> List(version("wikidata", "score 4"))),
					masterId -> Map("slave" -> List(version("merging", "0.4")))),
				properties = Map("geo_county" -> List("val 5")),
				properties_history = Map("geo_county" -> List(version("error", "val 5")))
			),
			Subject(
				id = subjectIds(5),
				master = masterId,
				name = Option("Name 6"),
				name_history = List(version("wikidata", "Name 6")),
				relations = Map(masterId -> Map("slave" -> "0.5")),
				relations_history = Map(masterId -> Map("slave" -> List(version("merging", "0.5")))),
				properties = Map(),
				properties_history = Map("gen_urls" -> List(version("implisense", "val 3"), version("human")))
			),
			Subject(
				id = subjectIds(6),
				master = masterId,
				name = Option("Name 7"),
				name_history = List(version("syntax error", "Name 7")),
				relations = Map(masterId -> Map("slave" -> "0.6")),
				properties = Map("prop 1" -> List("val x")),
				properties_history = Map("prop 1" -> List(version("wikidata", "val x")))
			),
			Subject(
				id = subjectIds(7),
				master = masterId,
				relations = Map(masterId -> Map("slave" -> "0.7"))
			),
			Subject(
				id = subjectIds(8),
				master = masterId,
				name = None,
				name_history = List(version("implisense", "Name 8"), version("human")),
				relations = Map(masterId -> Map("slave" -> "0.8"))
			),
			Subject(
				id = subjectIds(9),
				master = masterId,
				name = Option("Name 9"),
				name_history = List(version("implisense", "Name 9")),
				aliases = List("Alias 9"),
				aliases_history = List(version("implisense", "Alias 9")),
				relations = Map(masterId -> Map("slave" -> "0.9"))
			)
		)
	}

	def duplicatesWithoutMaster(): List[Subject] = {
		List(
			Subject(
				id = subjectIds.head,
				name = Option("Name 1"),
				name_history = List(version("implisense", "Name 1")),
				category = Option("Category 1"),
				category_history = List(version("implisense", "Category 1")),
				aliases = List("Alias 1", "Alias 2"),
				aliases_history = List(version("implisense", "Alias 1", "Alias 2")),
				relations = Map(subjectIds(1) -> Map("type 1" -> "score")),
				relations_history = Map(subjectIds(1) -> Map("type 1" -> List(version("implisense", "score")))),
				properties = Map("gen_urls" -> List("val 1", "val 2")),
				properties_history = Map("gen_urls" -> List(version("implisense", "val 1", "val 2")))
			),
			Subject(
				id = subjectIds(1),
				name = Option("Name 2"),
				name_history = List(version("wikidata", "Name 2")),
				category = Option("Category 2"),
				category_history = List(version("dbpedia", "Category 2")),
				relations = Map(
					subjectIds(2) -> Map("type 2" -> "score 2")),
				relations_history = Map(subjectIds(2) -> Map(
						"type 2" -> List(version("wikidata", "score 2")),
						"type 1" -> List(version("implisense", "score 2"), version("human")))),
				properties = Map("geo_city" -> List("val 1", "val 2")),
				properties_history = Map("geo_city" -> List(version("wikidata", "val 1", "val 2")))
			),
			Subject(
				id = subjectIds(2),
				name = Option("Name Human"),
				name_history = List(version("human", "Name Human")),
				category = Option("Category 3"),
				category_history = List(version("error", "Category 3")),
				aliases = List("Alias Human"),
				aliases_history = List(version("human", "Alias Human")),
				relations = Map(subjectIds(3) -> Map("type 3" -> "score 3")),
				relations_history = Map(subjectIds(3) -> Map("type 3" -> List(version("dbpedia", "score 3")))),
				properties = Map("gen_urls" -> List("val 3")),
				properties_history = Map("gen_urls" -> List(version("human", "val 3")))
			),
			Subject(
				id = subjectIds(3),
				category = None,
				category_history = List(version("implisense", "Category 4"), version("human")),
				aliases = List("Alias 6"),
				aliases_history = List(version("dbpedia", "Alias 6")),
				relations = Map(subjectIds(3) -> Map("type 3" -> "score 33")),
				relations_history = Map(subjectIds(3) -> Map("type 3" -> List(version("wikidata", "score 33")))),
				properties = Map("geo_city" -> List("val 1")),
				properties_history = Map("geo_city" -> List(version("wikidata", "val 1")))
			),
			Subject(
				id = subjectIds(4),
				name = Option("Name 5"),
				name_history = List(version("wikidata", "Name 5")),
				relations = Map(subjectIds(3) -> Map("type 4" -> "score 4")),
				relations_history = Map(subjectIds(3) -> Map("type 4" -> List(version("wikidata", "score 4")))),
				properties = Map("geo_county" -> List("val 5")),
				properties_history = Map("geo_county" -> List(version("error", "val 5")))
			),
			Subject(
				id = subjectIds(5),
				name = Option("Name 6"),
				name_history = List(version("wikidata", "Name 6")),
				properties = Map(),
				properties_history = Map("gen_urls" -> List(version("implisense", "val 3"), version("human")))
			),
			Subject(
				id = subjectIds(6),
				name = Option("Name 7"),
				name_history = List(version("syntax error", "Name 7")),
				properties = Map("prop 1" -> List("val x")),
				properties_history = Map("prop 1" -> List(version("wikidata", "val x")))
			),
			Subject(
				id = subjectIds(7)
			),
			Subject(
				id = subjectIds(8),
				name = None,
				name_history = List(version("implisense", "Name 8"), version("human"))
			),
			Subject(
				id = subjectIds(9),
				name = Option("Name 9"),
				name_history = List(version("implisense", "Name 9")),
				aliases = List("Alias 9"),
				aliases_history = List(version("implisense", "Alias 9"))
			)
		)
	}

	def propertyDuplicates(): List[Subject] = {
		List(
			Subject(
				id = subjectIds.head,
				properties = Map("gen_urls" -> List("val 1", "val 2")),
				properties_history = Map("gen_urls" -> List(version("implisense", "val 1", "val 2")))
			),
			Subject(
				id = subjectIds(1),
				properties = Map("geo_city" -> List("val 1")),
				properties_history = Map("geo_city" -> List(version("wikidata", "val 1")))
			),
			Subject(
				id = subjectIds(2),
				properties = Map("gen_urls" -> List("val 3")),
				properties_history = Map("gen_urls" -> List(version("human", "val 3")))
			),
			Subject(
				id = subjectIds(3),
				properties = Map("geo_city" -> List("val 1", "val 2")),
				properties_history = Map("geo_city" -> List(version("wikidata", "val 1", "val 2")))
			),
			Subject(
				id = subjectIds(4),
				properties = Map("geo_county" -> List("val 5")),
				properties_history = Map("geo_county" -> List(version("error", "val 5")))
			),
			Subject(
				id = subjectIds(5),
				properties = Map(),
				properties_history = Map("gen_urls" -> List(version("implisense", "val 3"), version("human")))
			),
			Subject(
				id = subjectIds(6),
				properties = Map("prop 1" -> List("val x")),
				properties_history = Map("prop 1" -> List(version("wikidata", "val x")))
			),
			Subject(
				id = subjectIds(7),
				properties = Map(),
				properties_history = Map()
			),
			Subject(
				id = subjectIds(8),
				properties = Map(),
				properties_history = Map()
			),
			Subject(
				id = subjectIds(9),
				properties = Map(),
				properties_history = Map()
			)
		)
	}

	def relationDuplicates(): List[Subject] = {
		List(
			Subject(
				id = subjectIds.head,
				relations = Map(subjectIds(1) -> Map("type 1" -> "score")),
				relations_history = Map(subjectIds(1) -> Map("type 1" -> List(version("implisense", "score"))))
			),
			Subject(
				id = subjectIds(1),
				relations = Map(subjectIds(2) -> Map("type 2" -> "score 2")),
				relations_history = Map(subjectIds(2) -> Map(
					"type 2" -> List(version("wikidata", "score 2")),
					"type 1" -> List(version("implisense", "score 2"), version("human"))
				))
			),
			Subject(
				id = subjectIds(2),
				relations = Map(subjectIds(3) -> Map("type 3" -> "score 3")),
				relations_history = Map(subjectIds(3) -> Map("type 3" -> List(version("dbpedia", "score 3"))))
			),
			Subject(
				id = subjectIds(3),
				relations = Map(subjectIds(3) -> Map("type 3" -> "score 33")),
				relations_history = Map(subjectIds(3) -> Map("type 3" -> List(version("wikidata", "score 33"))))
			),
			Subject(
				id = subjectIds(4),
				relations = Map(subjectIds(3) -> Map("type 4" -> "score 4")),
				relations_history = Map(subjectIds(3) -> Map("type 4" -> List(version("wikidata", "score 4"))))
			),
			Subject(
				id = subjectIds(5),
				relations = Map(masterId -> Map("slave" -> "0.5")),
				relations_history = Map(masterId -> Map("slave" -> List(version("merging", "0.5"))))
			),
			Subject(
				id = subjectIds(6),
				relations = Map(),
				relations_history = Map()
			),
			Subject(
				id = subjectIds(7),
				relations = Map(),
				relations_history = Map()
			),
			Subject(
				id = subjectIds(8),
				relations = Map(),
				relations_history = Map()
			),
			Subject(
				id = subjectIds(9),
				relations = Map(),
				relations_history = Map()
			)
		)
	}

	def simpleAttributeDuplicates(): List[Subject] = {
		List(
			Subject(
				id = subjectIds.head,
				master = masterId,
				name = Option("Name 1"),
				name_history = List(version("implisense", "Name 1")),
				category = Option("Category 1"),
				category_history = List(version("implisense", "Category 1")),
				aliases = List("Alias 1", "Alias 2"),
				aliases_history = List(version("implisense", "Alias 1", "Alias 2")),
				relations = Map(masterId -> Map("slave" -> "0.0"))
			),
			Subject(
				id = subjectIds(1),
				master = masterId,
				name = Option("Name 2"),
				name_history = List(version("wikidata", "Name 2")),
				category = Option("Category 2"),
				category_history = List(version("dbpedia", "Category 2")),
				relations = Map(masterId -> Map("slave" -> "0.1"))
			),
			Subject(
				id = subjectIds(2),
				master = masterId,
				name = Option("Name Human"),
				name_history = List(version("human", "Name Human")),
				category = Option("Category 3"),
				category_history = List(version("error", "Category 3")),
				aliases = List("Alias Human"),
				aliases_history = List(version("human", "Alias Human")),
				relations = Map(masterId -> Map("slave" -> "0.2"))
			),
			Subject(
				id = subjectIds(3),
				master = masterId,
				category = None,
				category_history = List(version("implisense", "Category 4"), version("human")),
				aliases = List("Alias 6"),
				aliases_history = List(version("dbpedia", "Alias 6")),
				relations = Map(masterId -> Map("slave" -> "0.3"))
			),
			Subject(
				id = subjectIds(4),
				master = masterId,
				name = Option("Name 5"),
				name_history = List(version("wikidata", "Name 5")),
				relations = Map(masterId -> Map("slave" -> "0.4"))
			),
			Subject(
				id = subjectIds(5),
				master = masterId,
				name = Option("Name 6"),
				name_history = List(version("wikidata", "Name 6")),
				relations = Map(masterId -> Map("slave" -> "0.5"))
			),
			Subject(
				id = subjectIds(6),
				master = masterId,
				name = Option("Name 7"),
				name_history = List(version("syntax error", "Name 7")),
				relations = Map(masterId -> Map("slave" -> "0.6"))
			),
			Subject(
				id = subjectIds(7),
				master = masterId,
				relations = Map(masterId -> Map("slave" -> "0.7"))
			),
			Subject(
				id = subjectIds(8),
				master = masterId,
				name = None,
				name_history = List(version("implisense", "Name 8"), version("human")),
				relations = Map(masterId -> Map("slave" -> "0.8"))
			),
			Subject(
				id = subjectIds(9),
				master = masterId,
				name = Option("Name 9"),
				name_history = List(version("implisense", "Name 9")),
				aliases = List("Alias 9"),
				aliases_history = List(version("implisense", "Alias 9")),
				relations = Map(masterId -> Map("slave" -> "0.9"))
			)
		)
	}

	def simpleAttributeDuplicatesWithoutMaster(): List[(Subject, Double)] = {
		List(
			(Subject(
				id = subjectIds.head,
				name = Option("Name 1"),
				name_history = List(version("implisense", "Name 1")),
				category = Option("Category 1"),
				category_history = List(version("implisense", "Category 1")),
				aliases = List("Alias 1", "Alias 2"),
				aliases_history = List(version("implisense", "Alias 1", "Alias 2"))
			), 0.0),
			(Subject(
				id = subjectIds(1),
				name = Option("Name 2"),
				name_history = List(version("wikidata", "Name 2")),
				category = Option("Category 2"),
				category_history = List(version("dbpedia", "Category 2"))
			), 0.1),
			(Subject(
				id = subjectIds(2),
				name = Option("Name Human"),
				name_history = List(version("human", "Name Human")),
				category = Option("Category 3"),
				category_history = List(version("error", "Category 3")),
				aliases = List("Alias Human"),
				aliases_history = List(version("human", "Alias Human"))
			), 0.2),
			(Subject(
				id = subjectIds(3),
				category = None,
				category_history = List(version("implisense", "Category 4"), version("human")),
				aliases = List("Alias 6"),
				aliases_history = List(version("dbpedia", "Alias 6"))
			), 0.3),
			(Subject(
				id = subjectIds(4),
				name = Option("Name 5"),
				name_history = List(version("wikidata", "Name 5"))
			), 0.4),
			(Subject(
				id = subjectIds(5),
				name = Option("Name 6"),
				name_history = List(version("wikidata", "Name 6"))
			), 0.5),
			(Subject(
				id = subjectIds(6),
				name = Option("Name 7"),
				name_history = List(version("syntax error", "Name 7"))
			), 0.6),
			(Subject(
				id = subjectIds(7)
			), 0.7),
			(Subject(
				id = subjectIds(8),
				name = None,
				name_history = List(version("implisense", "Name 8"), version("human"))
			), 0.8),
			(Subject(
				id = subjectIds(9),
				name = Option("Name 9"),
				name_history = List(version("implisense", "Name 9")),
				aliases = List("Alias 9"),
				aliases_history = List(version("implisense", "Alias 9"))
			), 0.9)
		)
	}

	def simpleDuplicateCandidates(): List[Duplicates] = {
		List(
			Duplicates(
				masterId,
				masterName,
				dataSource,
				(subjectIds, subjectNames)
					.zipped
					.toList
					.zipWithIndex
					.map { case ((id, name), index) => Candidate(id, name, s"0.$index".toDouble) }
			)
		)
	}

	def complexDuplicateCandidates(): List[Duplicates] = {
		List(
			Duplicates(
				subjectIds(7),
				subjectNames(7),
				dataSource,
				(subjectIds, subjectNames)
					.zipped
					.toList
					.slice(0, 5)
					.zipWithIndex
					.map { case ((id, name), index) => Candidate(id, name, s"0.$index".toDouble) }
			)
		)
	}

	def smallerDuplicatesWithoutMaster(): List[Subject] = {
		List(
			Subject(
				id = subjectIds.head,
				name = Option("Name 1"),
				name_history = List(version("implisense", "Name 1")),
				category = Option("Category 1"),
				category_history = List(version("implisense", "Category 1")),
				aliases = List("Alias 1", "Alias 2"),
				aliases_history = List(version("implisense", "Alias 1", "Alias 2")),
				relations = Map(subjectIds(1) -> Map("type 1" -> "score")),
				relations_history = Map(subjectIds(1) -> Map("type 1" -> List(version("implisense", "score")))),
				properties = Map("gen_urls" -> List("val 1", "val 2")),
				properties_history = Map("gen_urls" -> List(version("implisense", "val 1", "val 2")))
			),
			Subject(
				id = subjectIds(1),
				name = Option("Name 2"),
				name_history = List(version("wikidata", "Name 2")),
				category = Option("Category 2"),
				category_history = List(version("dbpedia", "Category 2")),
				relations = Map(
					subjectIds(2) -> Map("type 2" -> "score 2")),
				relations_history = Map(subjectIds(2) -> Map(
					"type 2" -> List(version("wikidata", "score 2")),
					"type 1" -> List(version("implisense", "score 2"), version("human")))),
				properties = Map("geo_city" -> List("val 1", "val 2")),
				properties_history = Map("geo_city" -> List(version("wikidata", "val 1", "val 2")))
			),
			Subject(
				id = subjectIds(2),
				name = Option("Name Human"),
				name_history = List(version("human", "Name Human")),
				category = Option("Category 3"),
				category_history = List(version("error", "Category 3")),
				aliases = List("Alias Human"),
				aliases_history = List(version("human", "Alias Human")),
				relations = Map(subjectIds(3) -> Map("type 3" -> "score 3")),
				relations_history = Map(subjectIds(3) -> Map("type 3" -> List(version("dbpedia", "score 3")))),
				properties = Map("gen_urls" -> List("val 3")),
				properties_history = Map("gen_urls" -> List(version("human", "val 3")))
			),
			Subject(
				id = subjectIds(3),
				category = None,
				category_history = List(version("implisense", "Category 4"), version("human")),
				aliases = List("Alias 6"),
				aliases_history = List(version("dbpedia", "Alias 6")),
				relations = Map(subjectIds(3) -> Map("type 3" -> "score 33")),
				relations_history = Map(subjectIds(3) -> Map("type 3" -> List(version("wikidata", "score 33")))),
				properties = Map("geo_city" -> List("val 1")),
				properties_history = Map("geo_city" -> List(version("wikidata", "val 1")))
			),
			Subject(
				id = subjectIds(4),
				name = Option("Name 5"),
				name_history = List(version("wikidata", "Name 5")),
				relations = Map(subjectIds(3) -> Map("type 4" -> "score 4")),
				relations_history = Map(subjectIds(3) -> Map("type 4" -> List(version("wikidata", "score 4")))),
				properties = Map("geo_county" -> List("val 5")),
				properties_history = Map("geo_county" -> List(version("error", "val 5")))
			),
			Subject(
				id = subjectIds(5),
				name = Option("Name 6"),
				name_history = List(version("wikidata", "Name 6")),
				properties = Map(),
				properties_history = Map("gen_urls" -> List(version("implisense", "val 3"), version("human")))
			),
			Subject(
				id = subjectIds(6),
				name = Option("Name 7"),
				name_history = List(version("syntax error", "Name 7")),
				properties = Map("prop 1" -> List("val x")),
				properties_history = Map("prop 1" -> List(version("wikidata", "val x")))
			)
		)
	}

	def existingSubjects(): List[Subject] = {
		List(
			Subject(
				id = subjectIds(7),
				master = masterId,
				relations = Map(masterId -> Map("slave" -> "1.0"))
			),
			Subject(
				id = masterId,
				relations = Map(subjectIds(7) -> Map("master" -> "1.0"))
			),
			Subject(
				id = subjectIds(8),
				master = Option(secondMasterIds.head),
				name = None,
				name_history = List(version("implisense", "Name 8"), version("human")),
				relations = Map(secondMasterIds.head -> Map("slave" -> "1.0"))
			),
			Subject(
				id = secondMasterIds.head,
				relations = Map(subjectIds(8) -> Map("master" -> "1.0"))
			),
			Subject(
				id = subjectIds(9),
				master = Option(secondMasterIds(1)),
				name = Option("Name 9"),
				name_history = List(version("implisense", "Name 9")),
				aliases = List("Alias 9"),
				aliases_history = List(version("implisense", "Alias 9")),
				relations = Map(secondMasterIds(1) -> Map("slave" -> "1.0"))
			),
			Subject(
				id = secondMasterIds(1),
				name = Option("Name 9"),
				aliases = List("Alias 9"),
				relations = Map(subjectIds(9) -> Map("master" -> "1.0"))
			)
		)
	}

	def resultSlaves(newUUIDs: List[UUID]): List[Subject] = {
		List(
			Subject(
				id = subjectIds.head,
				master = masterId,
				name = Option("Name 1"),
				name_history = List(version("implisense", "Name 1")),
				category = Option("Category 1"),
				category_history = List(version("implisense", "Category 1")),
				aliases = List("Alias 1", "Alias 2"),
				aliases_history = List(version("implisense", "Alias 1", "Alias 2")),
				relations = Map(
					masterId -> Map("slave" -> "0.0"),
					subjectIds(1) -> Map("type 1" -> "score"),
					subjectIds(7) -> Map("duplicate" -> "0.0")),
				relations_history = Map(
					subjectIds(1) -> Map("type 1" -> List(version("implisense", "score"))),
					masterId -> Map("slave" -> List(version("merging", "0.0")))),
				properties = Map("gen_urls" -> List("val 1", "val 2")),
				properties_history = Map("gen_urls" -> List(version("implisense", "val 1", "val 2")))
			),
			Subject(
				id = subjectIds(1),
				master = masterId,
				name = Option("Name 2"),
				name_history = List(version("wikidata", "Name 2")),
				category = Option("Category 2"),
				category_history = List(version("dbpedia", "Category 2")),
				relations = Map(
					subjectIds(2) -> Map("type 2" -> "score 2"),
					masterId -> Map("slave" -> "0.1"),
					subjectIds(7) -> Map("duplicate" -> "0.1")),
				relations_history = Map(
					subjectIds(2) -> Map(
						"type 2" -> List(version("wikidata", "score 2")),
						"type 1" -> List(version("implisense", "score 2"), version("human"))),
					masterId -> Map("slave" -> List(version("merging", "0.1")))),
				properties = Map("geo_city" -> List("val 1", "val 2")),
				properties_history = Map("geo_city" -> List(version("wikidata", "val 1", "val 2")))
			),
			Subject(
				id = subjectIds(2),
				master = masterId,
				name = Option("Name Human"),
				name_history = List(version("human", "Name Human")),
				category = Option("Category 3"),
				category_history = List(version("error", "Category 3")),
				aliases = List("Alias Human"),
				aliases_history = List(version("human", "Alias Human")),
				relations = Map(
					subjectIds(3) -> Map("type 3" -> "score 3"),
					masterId -> Map("slave" -> "0.2"),
					subjectIds(7) -> Map("duplicate" -> "0.2")),
				relations_history = Map(
					subjectIds(3) -> Map("type 3" -> List(version("dbpedia", "score 3"))),
					masterId -> Map("slave" -> List(version("merging", "0.2")))),
				properties = Map("gen_urls" -> List("val 3")),
				properties_history = Map("gen_urls" -> List(version("human", "val 3")))
			),
			Subject(
				id = subjectIds(3),
				master = masterId,
				category = None,
				category_history = List(version("implisense", "Category 4"), version("human")),
				aliases = List("Alias 6"),
				aliases_history = List(version("dbpedia", "Alias 6")),
				relations = Map(
					subjectIds(3) -> Map("type 3" -> "score 33"),
					masterId -> Map("slave" -> "0.3"),
					subjectIds(7) -> Map("duplicate" -> "0.3")),
				relations_history = Map(
					subjectIds(3) -> Map("type 3" -> List(version("wikidata", "score 33"))),
					masterId -> Map("slave" -> List(version("merging", "0.3")))),
				properties = Map("geo_city" -> List("val 1")),
				properties_history = Map("geo_city" -> List(version("wikidata", "val 1")))
			),
			Subject(
				id = subjectIds(4),
				master = masterId,
				name = Option("Name 5"),
				name_history = List(version("wikidata", "Name 5")),
				relations = Map(
					subjectIds(3) -> Map("type 4" -> "score 4"),
					masterId -> Map("slave" -> "0.4"),
					subjectIds(7) -> Map("duplicate" -> "0.4")),
				relations_history = Map(
					subjectIds(3) -> Map("type 4" -> List(version("wikidata", "score 4"))),
					masterId -> Map("slave" -> List(version("merging", "0.4")))),
				properties = Map("geo_county" -> List("val 5")),
				properties_history = Map("geo_county" -> List(version("error", "val 5")))
			),
			Subject(
				id = subjectIds(5),
				master = Option(newUUIDs.head),
				name = Option("Name 6"),
				name_history = List(version("wikidata", "Name 6")),
				relations = Map(newUUIDs.head -> Map("slave" -> "1.0")),
				relations_history = Map(newUUIDs.head -> Map("slave" -> List(version("merging", "1.0")))),
				properties = Map(),
				properties_history = Map("gen_urls" -> List(version("implisense", "val 3"), version("human")))
			),
			Subject(
				id = subjectIds(6),
				master = Option(newUUIDs(1)),
				name = Option("Name 7"),
				name_history = List(version("syntax error", "Name 7")),
				relations = Map(newUUIDs(1) -> Map("slave" -> "1.0")),
				properties = Map("prop 1" -> List("val x")),
				properties_history = Map("prop 1" -> List(version("wikidata", "val x")))
			),
			Subject(
				id = subjectIds(7),
				master = masterId,
				relations = Map(
					masterId -> Map("slave" -> "1.0"),
					subjectIds.head -> Map("duplicate" -> "0.0"),
					subjectIds(1) -> Map("duplicate" -> "0.1"),
					subjectIds(2) -> Map("duplicate" -> "0.2"),
					subjectIds(3) -> Map("duplicate" -> "0.3"),
					subjectIds(4) -> Map("duplicate" -> "0.4"))
			),
			Subject(
				id = subjectIds(7),
				master = masterId,
				relations = Map(masterId -> Map("slave" -> "1.0"))
			),
			Subject(
				id = subjectIds(8),
				master = Option(secondMasterIds.head),
				name = None,
				name_history = List(version("implisense", "Name 8"), version("human")),
				relations = Map(secondMasterIds.head -> Map("slave" -> "1.0"))
			),
			Subject(
				id = subjectIds(9),
				master = Option(secondMasterIds(1)),
				name = Option("Name 9"),
				name_history = List(version("implisense", "Name 9")),
				aliases = List("Alias 9"),
				aliases_history = List(version("implisense", "Alias 9")),
				relations = Map(secondMasterIds(1) -> Map("slave" -> "1.0"))
			)
		)
	}

	def resultMasters(newUUIDs: List[UUID]): List[Subject] = {
		List(
			Subject(
				id = masterId,
				name = mergedName(),
				category = mergedCategory(),
				aliases = reducedMergedAliases(),
				properties = mergedProperties(),
				relations = reducedMergesRelationsWithMaster()
			),
			Subject(
				id = newUUIDs.head,
				name = Option("Name 6"),
				relations = Map(subjectIds(5) -> Map("master" -> "1.0"))
			),
			Subject(
				id = newUUIDs(1),
				relations = Map(subjectIds(6) -> Map("master" -> "1.0"))
			),
			Subject(
				id = secondMasterIds.head,
				relations = Map(subjectIds(8) -> Map("master" -> "1.0"))
			),
			Subject(
				id = secondMasterIds(1),
				name = Option("Name 9"),
				aliases = List("Alias 9"),
				relations = Map(subjectIds(9) -> Map("master" -> "1.0"))
			)
		)
	}
	*/
}
// scalastyle:on
