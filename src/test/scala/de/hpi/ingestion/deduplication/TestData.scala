package de.hpi.ingestion.deduplication

import java.util.UUID

import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.models.{BlockEvaluation, FeatureEntry, PrecisionRecallDataTuple, ScoreConfig}
import de.hpi.ingestion.deduplication.blockingschemes.ListBlockingScheme
import de.hpi.ingestion.deduplication.models._
import de.hpi.ingestion.deduplication.similarity._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

// scalastyle:off line.size.limit
// scalastyle:off number.of.methods
object TestData {
	val idList = List.fill(8)(UUID.randomUUID())

	def bucketsList: Map[Int, List[Double]] = Map(
		10 -> List(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0),
		5 -> List(0.0, 0.2, 0.4, 0.6, 0.8, 1.0)
	)

	def testScores: List[Map[Double, Double]] = List(
		Map(0.5 -> 0.5, 0.79 -> 0.7),
		Map(0.5 -> 0.4, 0.79 -> 0.6)
	)

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

	def testData2(sc: SparkContext): RDD[(UUID, UUID)] = {
		sc.parallelize(Seq(
			(idList.head, idList(1)),
			(idList(2), idList(3)),
			(idList(4), idList(5)),
			(idList(6), idList(7))
		))
	}

	def trainingCandidates(sc: SparkContext): RDD[DuplicateCandidates] = {
		sc.parallelize(Seq(
			DuplicateCandidates(idList.head, List((idList(1), "test", 0.7))),
			DuplicateCandidates(idList(2), List((idList(3), "test", 0.8))),
			DuplicateCandidates(idList(4), List((idList(7), "test", 0.5))),
			DuplicateCandidates(idList(6), List((idList(5), "test", 0.6)))
		))
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

	def labeledPoints(sc: SparkContext): RDD[(Double, Double)] = {
		truePositives(sc).union(falsePositives(sc)).union(falseNegatives(sc))
	}

	def precisionRecallResults: List[PrecisionRecallDataTuple] = List(
		PrecisionRecallDataTuple(0.0,0.6666666666666666,1.0,0.8),
		PrecisionRecallDataTuple(0.1,0.5,0.5,0.5),
		PrecisionRecallDataTuple(0.2,0.5,0.5,0.5),
		PrecisionRecallDataTuple(0.3,0.5,0.5,0.5),
		PrecisionRecallDataTuple(0.4,0.5,0.5,0.5),
		PrecisionRecallDataTuple(0.5,0.5,0.5,0.5),
		PrecisionRecallDataTuple(0.6,0.6666666666666666,0.5,0.5714285714285715),
		PrecisionRecallDataTuple(0.7,1.0,0.5,0.6666666666666666),
		PrecisionRecallDataTuple(0.8,1.0,0.25,0.4),
		PrecisionRecallDataTuple(0.9,0.0,0.0,0.0),
		PrecisionRecallDataTuple(1.0,0.0,0.0,0.0)
	)

	def similarityMeasureStats: SimilarityMeasureStats = SimilarityMeasureStats(
		data = List(
			PrecisionRecallDataTuple(0.0,0.6666666666666666,1.0,0.8),
			PrecisionRecallDataTuple(0.2,0.5,0.5,0.5),
			PrecisionRecallDataTuple(0.4,0.5,0.5,0.5),
			PrecisionRecallDataTuple(0.6,0.6666666666666666,0.5,0.5714285714285715),
			PrecisionRecallDataTuple(0.8,1.0,0.25,0.4),
			PrecisionRecallDataTuple(1.0,0.0,0.0,0.0)
		),
		comment = Option("Naive Deduplication")
	)

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
		Subject(id = UUID.fromString("4fbc0340-4862-431f-9c28-a508234b8130"), name = Some("Volkswagen"), properties = Map("geo_city" -> List("Berlin", "Hamburg"), "geo_coords" -> List("52","11"), "gen_income" -> List("1234"))),
		Subject(id = UUID.fromString("f3c410f2-5acf-4e09-b7b6-d357285f0354"), name = Some("Volkswagen AG"), properties = Map("geo_city" -> List("Berlin", "New York"), "gen_income" -> List("12"))),
		Subject(id = UUID.fromString("392c1b32-4ff2-4d1f-aa9f-e172c9f64754"), name = Some("Audi GmbH"), properties = Map("geo_city" -> List("Berlin"), "geo_coords" -> List("52","13"), "gen_income" -> List("33"))),
		Subject(id = UUID.fromString("15954b5e-56cb-4b54-bf22-e7b654f2f34e"), name = Some("Audy GmbH"), properties = Map("geo_city" -> List("New York", "Hamburg"), "geo_coords" -> List("53","14"), "gen_income" -> List("600"))),
		Subject(id = UUID.fromString("b275ebc4-a481-4248-b7dd-3b6dd6fb804a"), name = Some("Porsche"), properties = Map("geo_coords" -> List("52","13"), "gen_sectors" -> List("cars", "music", "games"))),
		Subject(id = UUID.fromString("b275ebc4-a481-4248-b7dd-3b6dd6fb804b"), name = Some("Ferrari"), properties = Map("geo_coords" -> List("53","14"), "gen_sectors" -> List("games", "cars", "music")))
	)

	def subjectBlocks(testSubjects: List[Subject], sc: SparkContext): RDD[Block] = {
		sc.parallelize(List(
			Block(key = "Aud", subjects = List(testSubjects(2)), staging = List(testSubjects(3))),
			Block(key = "Vol", subjects = testSubjects.take(1), staging = List(testSubjects(1))),
			Block(key = "undefined", subjects = testSubjects.take(2), staging = List(testSubjects(4)))
		))
	}

	def emptySubject: Subject = Subject()

	def testVersion(sc: SparkContext): Version = Version("SomeTestApp", Nil, sc, false)

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

	def trueDuplicateCandidates(subjects: List[Subject], stagings: List[Subject]): List[DuplicateCandidates] = {
		val stagingTable = "subject_wikidata"
		List(
			DuplicateCandidates(
				subjects.head.id,
				List((stagings.head.id, stagingTable, 0.7117948717948718))),
			DuplicateCandidates(
				subjects(1).id,
				List((stagings(1).id, stagingTable, 0.9446913580246914))))
	}

	def duplicateCandidates(subjects: List[Subject]): List[DuplicateCandidates] = {
		val stagingTable = "subject_wikidata"
		List(
			DuplicateCandidates(
				subjects.head.id,
				List((subjects(1).id, stagingTable, 0.7117948717948718), (subjects(4).id, stagingTable, 0.5761904761904763))),
			DuplicateCandidates(
				subjects(1).id,
				List((subjects(4).id, stagingTable, 0.5654212454212454))),
			DuplicateCandidates(
				subjects(2).id,
				List((subjects(3).id, stagingTable, 0.9446913580246914))))
	}

	def testConfig(key: String = "name"): Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]] = {
		Map(
			key -> List(
				ScoreConfig(MongeElkan, 0.8),
				ScoreConfig(JaroWinkler, 0.7)))
	}

	def simpleTestConfig: Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]] = {
		Map("name" -> List(ScoreConfig(MongeElkan, 0.8)))
	}

	def complexTestConfig: Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]] = {
		Map(
			"name" -> List(ScoreConfig(MongeElkan, 0.8), ScoreConfig(JaroWinkler, 0.7)),
			"geo_city" -> List(ScoreConfig(Jaccard, 1)),
			"geo_coords" -> List(ScoreConfig(EuclidianDistance, 1)),
			"gen_income" -> List(ScoreConfig(RelativeNumbersSimilarity, 1))
		)
	}

	def testConfigScore(subject1: Subject, subject2: Subject): Double = List(
		MongeElkan.compare(subject1.name.get, subject2.name.get) * 0.8,
		JaroWinkler.compare(subject1.name.get, subject2.name.get) * 0.7
	).sum / (0.8 + 0.7)

	def simpleTestConfigScore(subject: Subject, staging: Subject): Double = MongeElkan.compare(subject.name.get, staging.name.get)

	def complexTestConfigScore(subject: Subject, staging: Subject): Double = List(
		MongeElkan.compare(subject.name.get, staging.name.get) * 0.8,
		JaroWinkler.compare(subject.name.get, staging.name.get) * 0.7,
		Jaccard.compare("Berlin", "Berlin"),
		RelativeNumbersSimilarity.compare("1234", "12")
	).sum / (0.8 + 0.7 + 1 + 1)

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

	def goldStandard(subjects: List[Subject], stagings: List[Subject]): Set[(UUID, UUID)] = Set(
		(subjects.head.id, stagings.head.id),
		(subjects(1).id, stagings.last.id)
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
					BlockStats("Berlin", 2, 1, 0.5),
					BlockStats("Hamburg", 1, 1),
					BlockStats("New York", 0, 2),
					BlockStats("undefined", 2, 0)),
				Option("Blocking; accuracy: 0.5")),
			BlockEvaluation(
				null,
				"SimpleBlocking",
				Set(
					BlockStats("Vol", 1, 1, 1.0),
					BlockStats("Aud", 1, 1, 1.0),
					BlockStats("Por", 1, 0),
					BlockStats("Fer", 1, 0)),
				Option("Blocking; accuracy: 1.0"))
		))
	}

	def filteredBlockEvaluation(sc: SparkContext): RDD[BlockEvaluation] = {
		sc.parallelize(List(
			BlockEvaluation(
				null,
				"GeoBlocking",
				Set(
					BlockStats("Berlin", 2, 1, 0.5),
					BlockStats("Hamburg", 1, 1),
					BlockStats("New York", 0, 2)),
				Option("Blocking; accuracy: 0.5")),
			BlockEvaluation(
				null,
				"SimpleBlocking",
				Set(
					BlockStats("Vol", 1, 1, 1.0),
					BlockStats("Aud", 1, 1, 1.0),
					BlockStats("Por", 1, 0),
					BlockStats("Fer", 1, 0)),
				Option("Blocking; accuracy: 1.0"))))
	}

	def blockEvaluationWithComment(sc: SparkContext): RDD[BlockEvaluation] = {
		sc.parallelize(List(
			BlockEvaluation(
				null,
				"GeoBlocking",
				Set(
					BlockStats("Berlin", 2, 1, 0.5),
					BlockStats("Hamburg", 1, 1),
					BlockStats("New York", 0, 2),
					BlockStats("undefined", 2, 0)),
				Option("Test comment; accuracy: 0.5")),
			BlockEvaluation(
				null,
				"SimpleBlocking",
				Set(
					BlockStats("Vol", 1, 1, 1.0),
					BlockStats("Aud", 1, 1, 1.0),
					BlockStats("Por", 1, 0),
					BlockStats("Fer", 1, 0)),
				Option("Test comment; accuracy: 1.0"))
		))
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
	def geoCoordsBlockingScheme: List[List[String]] = List(List("52,11"), List("undefined"), List("52,13"), List("53,14"))
	def randomBlockingScheme: List[List[String]] = List(List("-534"), List("42"), List("-545"), List("43"), List("400"), List("75"))
	def mapBlockingScheme: List[List[String]] = List(List("Vol"), List("Vol"), List("Aud"), List("Aud"), List("Por"), List("Fer"))

	def features(sc: SparkContext): RDD[FeatureEntry] = sc.parallelize(List(
		FeatureEntry(subject = Subject(id = idList.head), staging = Subject(id = idList(1))),
		FeatureEntry(subject = Subject(id = idList(2)), staging = Subject(id = idList(3))),
		FeatureEntry(subject = Subject(id = idList(4)), staging = Subject(id = idList(5)))
	))

	def goldStandard(sc: SparkContext): RDD[(UUID, UUID)] = sc.parallelize(List(
		(idList.head, idList(1)), (idList(2), idList(3)), (idList(5), idList(6))
	))

	def labeledFeatures(sc: SparkContext): RDD[FeatureEntry] = sc.parallelize(List(
		FeatureEntry(subject = Subject(id = idList.head), staging = Subject(id = idList(1)), correct = true),
		FeatureEntry(subject = Subject(id = idList(2)), staging = Subject(id = idList(3)), correct = true),
		FeatureEntry(subject = Subject(id = idList(4)), staging = Subject(id = idList(5)))
	))

	def requiredSettings: List[String] = {
		List("keyspaceSubjectTable", "subjectTable", "keyspaceStagingTable", "stagingTable", "keyspaceStatsTable", "statsTable")
	}

	def featureEntries: List[FeatureEntry] = {
		List(
			FeatureEntry(null, null, null, Map("a" -> List(0.2, 0.4, 0.1)), false),
			FeatureEntry(null, null, null, Map("a" -> List(0.1, 0.3, 0.2)), false),
			FeatureEntry(null, null, null, Map("a" -> List(0.3, 0.5, 0.15)), false),
			FeatureEntry(null, null, null, Map("a" -> List(0.4, 0.3, 0.1)), false),
			FeatureEntry(null, null, null, Map("a" -> List(0.345, 0.6, 0.3)), false),
			FeatureEntry(null, null, null, Map("a" -> List(0.6, 0.7, 0.5)), true),
			FeatureEntry(null, null, null, Map("a" -> List(0.124, 0.4, 0.3)), false),
			FeatureEntry(null, null, null, Map("a" -> List(0.78, 0.9, 0.5)), true),
			FeatureEntry(null, null, null, Map("a" -> List(0.9, 0.7, 0.6)), true),
			FeatureEntry(null, null, null, Map("a" -> List(0.85, 0.6, 0.3)), true))
	}

	def dbpediaEntries: List[Subject] = {
		List(
			Subject(id = idList.head, name = Option("Audi")),
			Subject(id = idList(2), name = Option("Volkswagen")),
			Subject(id = idList(4), name = Option("Porsche")))
	}

	def wikidataEntries: List[Subject] = {
		List(
			Subject(id = idList(1), name = Option("Audi AG")),
			Subject(id = idList(3), name = Option("Volkswagen AG")),
			Subject(id = idList(5), name = Option("Porsche AG")))
	}
}
// scalastyle:on line.size.limit
// scalastyle:on number.of.methods
