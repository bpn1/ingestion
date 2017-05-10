package de.hpi.ingestion.deduplication

import java.util.UUID

import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.blockingschemes.ListBlockingScheme
import de.hpi.ingestion.deduplication.models._
import de.hpi.ingestion.deduplication.similarity.{JaroWinkler, MongeElkan, SimilarityMeasure}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

// scalastyle:off line.size.limit
// scalastyle:off number.of.methods
object TestData {
	val idList = List.fill(8)(UUID.randomUUID())
	def testData(sc: SparkContext): RDD[((UUID, UUID), Double)] = {
		sc.parallelize(Seq(
			((idList.head, idList(1)), 1.0),
			((idList(2), idList(3)), 1.0),
			((idList(4), idList(5)), 1.0),
			((idList(6), idList(7)), 1.0)
		))
	}

	def trainingData(sc: SparkContext): RDD[((UUID, UUID), Double)] = {
		sc.parallelize(Seq(
			((idList.head, idList(1)), 0.7),
			((idList(2), idList(3)), 0.8),
			((idList(4), idList(7)), 0.5),
			((idList(6), idList(5)), 0.6)
		))
	}

	def precisionRecallResults(sc: SparkContext) : RDD[PrecisionRecallDataTuple] = {
		sc.parallelize(
			List(
				PrecisionRecallDataTuple(0.0, 0.6666666666666666, 1, 0.8),
				PrecisionRecallDataTuple(0.5, 0.5, 0.5, 0.5),
				PrecisionRecallDataTuple(0.6, 0.6666666666666666, 0.5, 0.5714285714285715),
				PrecisionRecallDataTuple(0.7, 1, 0.5, 0.6666666666666666),
				PrecisionRecallDataTuple(0.8, 1, 0.25, 0.4)
			)
		)
	}

	def truePositives(sc: SparkContext): RDD[(Double, Double)] = {
		sc.parallelize(List((0.7, 1.0), (0.8, 1.0)))
	}

	def falsePositives(sc: SparkContext): RDD[(Double, Double)] = {
		sc.parallelize(List((0.5, 0.0), (0.6, 0.0)))
	}

	def falseNegatives(sc: SparkContext): RDD[(Double, Double)] = {
		sc.parallelize(List.fill(2)((0: Double, 1: Double)))
	}

	val dbpediaList = List(
		Subject(
			name = Option("dbpedia_1"),
			properties = Map("id_dbpedia" -> List("dbpedia_1"), "id_wikidata" -> List("Q1"))
		),
		Subject(
			name = Option("dbpedia_2"),
			properties = Map("id_dbpedia" -> List("dbpedia_2"), "id_wikidata" -> List("Q2"))
		),
		Subject(
			name = Option("dbpedia_3"),
			properties = Map("id_wikidata" -> List("Q3"))
		),
		Subject(name = Option("dbpedia_4"))
	)

	val wikidataList = List(
		Subject(
			name = Option("wikidata_1"),
			properties = Map("id_dbpedia" -> List("dbpedia_1"), "id_wikidata" -> List(""))
		),
		Subject(
			name = Option("wikidata_2"),
			properties = Map("id_dbpedia" -> List("dbpedia_2"), "id_wikidata" -> List("Q2"))
		),
		Subject(
			name = Option("wikidata_3"),
			properties = Map("id_dbpedia" -> List("dbpedia_3"), "id_wikidata" -> List("Q3"))
		),
		Subject(name = Option("wikidata_4"))
	)

	def propertyKeyDBpedia(sc: SparkContext): RDD[(String, Subject)] = {
		sc.parallelize(Seq(
			("dbpedia_1", dbpediaList.head),
			("dbpedia_2", dbpediaList(1))
		))
	}

	def joinedWikiData(sc: SparkContext): RDD[(UUID, UUID)] = {
		sc.parallelize(Seq(
			(dbpediaList(1).id, wikidataList(1).id),
			(dbpediaList(2).id, wikidataList(2).id)
		))
	}

	def joinedDBpediaWikiData(sc: SparkContext): RDD[(UUID, UUID)] = {
		sc.parallelize(Seq(
			(dbpediaList.head.id, wikidataList.head.id),
			(dbpediaList(1).id, wikidataList(1).id),
			(dbpediaList(2).id, wikidataList(2).id)
		))
	}

	def testSubjects: List[Subject] = List(
		Subject(name = Some("Volkswagen"), properties = Map("geo_city" -> List("Berlin", "Hamburg"), "geo_coords" -> List("52","11"), "gen_income" -> List("1234"))),
		Subject(name = Some("Volkswagen AG"), properties = Map("geo_city" -> List("Berlin", "New York"), "gen_income" -> List("12"))),
		Subject(name = Some("Audi GmbH"), properties = Map("geo_city" -> List("Berlin"), "geo_coords" -> List("52","13"), "gen_income" -> List("33"))),
		Subject(name = Some("Audy GmbH"), properties = Map("geo_city" -> List("New York", "Hamburg"), "geo_coords" -> List("53","14"), "gen_income" -> List("600"))),
		Subject(name = Some("Porsche"), properties = Map("geo_coords" -> List("52","13"), "gen_sectors" -> List("cars", "music", "games"))),
		Subject(name = Some("Ferrari"), properties = Map("geo_coords" -> List("53","14"), "gen_sectors" -> List("games", "cars", "music")))
	)

	def subjectBlocks(testSubjects: List[Subject], sc: SparkContext): RDD[Block] = {
		sc.parallelize(List(
			Block(key = "Aud", subjects = List(testSubjects(2)), staging = List(testSubjects(3))),
			Block(key = "Vol", subjects = testSubjects.take(1), staging = List(testSubjects(1))),
			Block(key = "undefined", subjects = testSubjects.take(2), staging = List(testSubjects(4)))
		))
	}

	def emptySubject: Subject = Subject()

	def testVersion(sc: SparkContext): Version = Version("SomeTestApp", Nil, sc)

	def parsedConfig: Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]] = {
		Map(
			"name" -> List(
				ScoreConfig(similarityMeasure = MongeElkan, weight = 0.8),
				ScoreConfig(similarityMeasure = JaroWinkler, weight = 0.7)))
	}

	def testDuplicates(testSubjects: List[Subject]): List[(Subject, Subject, Double)] = {
		List(
			(testSubjects.head, testSubjects(1), 0.7117948717948718),
			(testSubjects.head, testSubjects(4), 0.5761904761904763),
			(testSubjects(1), testSubjects(4), 0.5654212454212454),
			(testSubjects(2), testSubjects(3), 0.9446913580246914))
	}

	def trueDuplicateCandidates(subjects: List[Subject]): List[DuplicateCandidates] = {
		val stagingTable = "subject_wikidata"
		List(
			DuplicateCandidates(
				subjects.head.id,
				List((subjects(1), stagingTable, 0.7117948717948718))),
			DuplicateCandidates(
				subjects(2).id,
				List((subjects(3), stagingTable, 0.9446913580246914))))
	}

	def duplicateCandidates(subjects: List[Subject]): List[DuplicateCandidates] = {
		val stagingTable = "subject_wikidata"
		List(
			DuplicateCandidates(
				subjects.head.id,
				List((subjects(1), stagingTable, 0.7117948717948718), (subjects(4), stagingTable, 0.5761904761904763))),
			DuplicateCandidates(
				subjects(1).id,
				List((subjects(4), stagingTable, 0.5654212454212454))),
			DuplicateCandidates(
				subjects(2).id,
				List((subjects(3), stagingTable, 0.9446913580246914))))
	}

	def testConfig(key: String = "name"): Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]] = {
		Map(
			key -> List(
				ScoreConfig(similarityMeasure = MongeElkan, weight= 0.8),
				ScoreConfig(similarityMeasure = JaroWinkler, weight= 0.7)))
	}

	def testSubjectScore(subject1: Subject, subject2: Subject): Double = List(
		MongeElkan.compare(subject1.name.get, subject2.name.get) * 0.8,
		JaroWinkler.compare(subject1.name.get, subject2.name.get) * 0.7
	).sum / (0.8 + 0.7)

	def testCompareScore(subject1: Subject, subject2: Subject, simMeasure: SimilarityMeasure[String], scoreConfig: ScoreConfig[String, SimilarityMeasure[String]]): Double = {
		simMeasure.compare(subject1.name.get, subject2.name.get, scoreConfig.scale) * scoreConfig.weight
	}

	def expectedCompareStrategies: List[(List[String], List[String], ScoreConfig[String, SimilarityMeasure[String]]) => Double] = List(
		CompareStrategy.singleStringCompare, CompareStrategy.coordinatesCompare, CompareStrategy.defaultCompare
	)

	def testCompareInput: (List[String], List[String], ScoreConfig[String, SimilarityMeasure[String]]) = {
		(List("very", "generic", "values"), List("even", "more", "values"), testConfig().head._2.head)
	}

	def cityBlockingScheme: ListBlockingScheme = {
		val scheme = new ListBlockingScheme()
		scheme.setAttributes("geo_city")
		scheme.tag = "GeoBlocking"
		scheme
	}

	def subjects: List[Subject] = List(
		Subject(name = Some("Volkswagen"), properties = Map("geo_city" -> List("Berlin", "Hamburg"), "geo_coords" -> List("52","11"), "gen_income" -> List("1234"))),
		Subject(name = Some("Audi GmbH"), properties = Map("geo_city" -> List("Berlin"), "geo_coords" -> List("52","13"), "gen_income" -> List("33"))),
		Subject(name = Some("Porsche"), properties = Map("geo_coords" -> List("52","13"), "gen_sectors" -> List("cars", "music", "games"))),
		Subject(name = Some("Ferrari"), properties = Map("geo_coords" -> List("53","14"), "gen_sectors" -> List("games", "cars", "music")))
	)

	def stagings: List[Subject] = List(
		Subject(name = Some("Volkswagen AG"), properties = Map("geo_city" -> List("Berlin", "New York"), "gen_income" -> List("12"))),
		Subject(name = Some("Audy GmbH"), properties = Map("geo_city" -> List("New York", "Hamburg"), "geo_coords" -> List("53","14"), "gen_income" -> List("600")))
	)

	def flatSubjectBlocks(sc: SparkContext, subjects: List[Subject]): RDD[((String, String), Subject)] = {
		sc.parallelize(List(
			(("Berlin", "GeoBlocking"), subjects.head),
			(("Berlin", "GeoBlocking"), subjects(1)),
			(("Hamburg", "GeoBlocking"), subjects.head),
			(("undefined", "GeoBlocking"), subjects(2)),
			(("undefined", "GeoBlocking"), subjects(3)),
			(("Vol", "SimpleBlocking"), subjects.head),
			(("Aud", "SimpleBlocking"), subjects(1)),
			(("Por", "SimpleBlocking"), subjects(2)),
			(("Fer", "SimpleBlocking"), subjects(3))
		))
	}

	def flatStagingBlocks(sc: SparkContext, staging: List[Subject]): RDD[((String, String), Subject)] = {
		sc.parallelize(List(
			(("Berlin", "GeoBlocking"), staging.head),
			(("Hamburg", "GeoBlocking"), staging.last),
			(("New York", "GeoBlocking"), staging.head),
			(("New York", "GeoBlocking"), staging.last),
			(("Vol", "SimpleBlocking"), staging.head),
			(("Aud", "SimpleBlocking"), staging.last)
		))
	}

	def flatBlocks(sc: SparkContext, subjects: List[Subject], staging: List[Subject]): RDD[((String, String), Subject)] = {
		sc.parallelize(List(
			(("Berlin", "GeoBlocking"), staging.head),
			(("Berlin", "GeoBlocking"), subjects.head),
			(("Berlin", "GeoBlocking"), subjects(1)),
			(("Hamburg", "GeoBlocking"), subjects.head),
			(("Hamburg", "GeoBlocking"), staging.last),
			(("New York", "GeoBlocking"), staging.head),
			(("New York", "GeoBlocking"), staging.last),
			(("undefined", "GeoBlocking"), subjects(2)),
			(("undefined", "GeoBlocking"), subjects(3)),
			(("Vol", "SimpleBlocking"), subjects.head),
			(("Vol", "SimpleBlocking"), staging.head),
			(("Aud", "SimpleBlocking"), subjects(1)),
			(("Aud", "SimpleBlocking"), staging.last),
			(("Por", "SimpleBlocking"), subjects(2)),
			(("Fer", "SimpleBlocking"), subjects(3))
		))
	}

	def groupedBlocks(sc: SparkContext, subjects: List[Subject], stagings: List[Subject]): RDD[(String, Block)] = {
		sc.parallelize(List(
			("GeoBlocking", Block(id = null, key = "Berlin", subjects = subjects.take(2), staging = stagings.take(1))),
			("GeoBlocking", Block(id = null, key = "Hamburg", subjects = subjects.take(1), staging = List(stagings.last))),
			("GeoBlocking", Block(id = null, key = "New York", staging = stagings)),
			("GeoBlocking", Block(id = null, key = "undefined", subjects = subjects.slice(2, 4))),
			("SimpleBlocking", Block(id = null, key = "Vol", subjects = subjects.take(1), staging = stagings.take(1))),
			("SimpleBlocking", Block(id = null, key = "Aud", subjects = List(subjects(1)), staging = List(stagings.last))),
			("SimpleBlocking", Block(id = null, key = "Por", subjects = List(subjects(2)))),
			("SimpleBlocking", Block(id = null, key = "Fer", subjects = List(subjects(3))))
		))
	}

	def groupedAndFilteredBlocks(sc: SparkContext, subjects: List[Subject], stagings: List[Subject]): RDD[(String, Block)] = {
		sc.parallelize(List(
			("GeoBlocking", Block(id = null, key = "Berlin", subjects = subjects.take(2), staging = stagings.take(1))),
			("GeoBlocking", Block(id = null, key = "Hamburg", subjects = subjects.take(1), staging = List(stagings.last))),
			("GeoBlocking", Block(id = null, key = "New York", staging = stagings)),
			("SimpleBlocking", Block(id = null, key = "Vol", subjects = subjects.take(1), staging = stagings.take(1))),
			("SimpleBlocking", Block(id = null, key = "Aud", subjects = List(subjects(1)), staging = List(stagings.last))),
			("SimpleBlocking", Block(id = null, key = "Por", subjects = List(subjects(2)))),
			("SimpleBlocking", Block(id = null, key = "Fer", subjects = List(subjects(3))))
		))
	}

	def blockEvaluation(sc: SparkContext): RDD[BlockEvaluation] = {
		sc.parallelize(List(
			BlockEvaluation(
				null,
				"GeoBlocking",
				Set(
					BlockStats("Berlin", 2, 1),
					BlockStats("Hamburg", 1, 1),
					BlockStats("New York", 0, 2),
					BlockStats("undefined", 2, 0)),
				Option("Blocking")),
			BlockEvaluation(
				null,
				"SimpleBlocking",
				Set(
					BlockStats("Vol", 1, 1),
					BlockStats("Aud", 1, 1),
					BlockStats("Por", 1, 0),
					BlockStats("Fer", 1, 0)),
				Option("Blocking"))))
	}

	def filteredBlockEvaluation(sc: SparkContext): RDD[BlockEvaluation] = {
		sc.parallelize(List(
			BlockEvaluation(
				null,
				"GeoBlocking",
				Set(
					BlockStats("Berlin", 2, 1),
					BlockStats("Hamburg", 1, 1),
					BlockStats("New York", 0, 2)),
				Option("Blocking")),
			BlockEvaluation(
				null,
				"SimpleBlocking",
				Set(
					BlockStats("Vol", 1, 1),
					BlockStats("Aud", 1, 1),
					BlockStats("Por", 1, 0),
					BlockStats("Fer", 1, 0)),
				Option("Blocking"))))
	}

	def blockEvaluationWithComment(sc: SparkContext): RDD[BlockEvaluation] = {
		sc.parallelize(List(
			BlockEvaluation(
				null,
				"GeoBlocking",
				Set(
					BlockStats("Berlin", 2, 1),
					BlockStats("Hamburg", 1, 1),
					BlockStats("New York", 0, 2),
					BlockStats("undefined", 2, 0)),
				Option("Test comment")),
			BlockEvaluation(
				null,
				"SimpleBlocking",
				Set(
					BlockStats("Vol", 1, 1),
					BlockStats("Aud", 1, 1),
					BlockStats("Por", 1, 0),
					BlockStats("Fer", 1, 0)),
				Option("Test comment"))))
	}

	def simpleBlockingScheme: List[List[String]] = List(List("Vol"), List("Vol"), List("Aud"), List("Aud"), List("Por"), List("Fer"))
	def listBlockingScheme: List[List[String]] = List(
		List("Berlin", "Hamburg", "1234"),
		List("Berlin", "New York", "12"),
		List("Berlin", "33"),
		List("New York", "Hamburg", "600"),
		List("undefined"),
		List("undefined")
	)
	def mapBlockingScheme: List[List[String]] = List(List("Vol"), List("Vol"), List("Aud"), List("Aud"), List("Por"), List("Fer"))

	def requiredSettings: List[String] = {
		List("keyspaceSubjectTable", "subjectTable", "keyspaceStagingTable", "stagingTable", "keyspaceStatsTable", "statsTable")
	}
}
// scalastyle:on line.size.limit
// scalastyle:on number.of.methods
