package de.hpi.ingestion.deduplication

import java.util.UUID
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.models.{BlockEvaluation, FeatureEntry, PrecisionRecallDataTuple}
import de.hpi.ingestion.deduplication.blockingschemes.ListBlockingScheme
import de.hpi.ingestion.deduplication.models._
import de.hpi.ingestion.deduplication.models.config.{AttributeConfig, SimilarityMeasureConfig}
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

	def subjectBlocks(sc: SparkContext): RDD[Block] = {
		sc.parallelize(Seq(
			Block(key = "Audi ", subjects = List(subjects(1)), staging = List(stagings(1))),
			Block(key = "Volks", subjects = subjects.take(1), staging = stagings.take(1)),
			Block(key = "undefined", subjects = subjects.takeRight(2), staging = Nil)
		))
	}

	def emptySubject: Subject = Subject()

	def testVersion(sc: SparkContext): Version = Version("SomeTestApp", Nil, sc, false)

	def filteredDuplicates(sc: SparkContext): RDD[(Subject, Subject, Double)] = {
		sc.parallelize(Seq(
			(subjects.head, stagings.head, 0.9769230769230769),
			(subjects(1), stagings(1), 1.0)
		))
	}

	def testDuplicates(sc: SparkContext): RDD[(Subject, Subject, Double)] = {
		sc.parallelize(Seq(
			(subjects.head, stagings.head, 0.9769230769230769),
			(subjects(1), stagings(1), 1.0),
			(subjects(1), stagings.head, 0.5)
		))
	}

	def testDuplicateStats: (Traversable[(UUID, UUID)], (String, Set[BlockStats])) = {
		(List(
			(
				UUID.fromString("974c4495-52fd-445c-b09b-8b769bfb4212"),
				UUID.fromString("4fbc0340-4862-431f-9c28-a508234b8130")
			), (
				UUID.fromString("f2d98c16-2ac2-4cb7-bd65-4fcb17178060"),
				UUID.fromString("413c5711-67d0-1151-1077-b000000000b5")
			)
		), (
			"key",
			Set(BlockStats("tag",4,2,0.25))
		)
		)
	}


	def createdDuplicateCandidates(subjects: List[Subject], stagings: List[Subject]): List[DuplicateCandidates] = {
		val stagingTable = "subject_wikidata"
		List(
			DuplicateCandidates(
				subjects.head.id,
				List((stagings.head.id, stagingTable, 0.9769230769230769))
			),
			DuplicateCandidates(
				subjects(1).id,
				List((stagings(1).id, stagingTable, 1.0), (stagings.head.id, stagingTable, 0.5))
			)
		)
	}

	def testDuplicateStatsRDD(sc: SparkContext): RDD[(Traversable[(UUID, UUID)], (String, Set[BlockStats]))] = {
		sc.parallelize(List((List(
			(
				UUID.fromString("974c4495-52fd-445c-b09b-8b769bfb4212"),
				UUID.fromString("4fbc0340-4862-431f-9c28-a508234b8130")
			), (
				UUID.fromString("0ef813fa-55d9-4712-a41d-46a810fca8c9"),
				UUID.fromString("413c5711-67d0-1151-1077-b000000000b5")
				)
		), (
			"key1",
			Set(BlockStats("tag1",4,2,0.25))
		)
		),(List(
			(
				UUID.fromString("0ef813fa-55d9-4712-a41d-46a810fca8c9"),
				UUID.fromString("413c5711-67d0-1151-1077-b000000000b5")
			)
		), (
			"key2",
			Set(BlockStats("tag2",1,1,1.0))
		)
		)) )
	}

	def expectedPairwiseCompleteness: Map[String, Double] = {
		Map("key1" -> 1.0, "key2" -> 0.5, "sum" -> 1.0)
	}

	def expectedBlockCount: Map[String, Int] = {
		Map("testTag1" -> 2, "testTag2" -> 1, "sum" -> 3)
	}

	def expectedCompareCount: Map[String, BigInt] = {
		Map("testTag1" -> 5, "testTag2" -> 15,"sum" -> 17)
	}

	def blocks(sc: SparkContext): RDD[((String, String), (Iterable[UUID], Iterable[UUID]))] = {
		sc.parallelize(List(
			(
				("anyKey", "testTag1"),
				(
					List(idList.head),
					List(idList.last)
				)
			), (
				("Anakin", "testTag2"),
				(
					List(idList.head, idList(1), idList(2), idList(3), idList(4)),
					List(idList.last, idList(4), idList(6))
				)
			), (
				("anotherKey", "testTag1"),
				(
					List(idList(4), idList(5)),
					List(idList(6), idList.last)
				)
			)
		))
	}

	def trueDuplicateCandidates(subjects: List[Subject], stagings: List[Subject]): List[DuplicateCandidates] = {
		val stagingTable = "subject_wikidata"
		List(
			DuplicateCandidates(
				subjects.head.id,
				List((stagings.head.id, stagingTable, 0.9769230769230769))
			),
			DuplicateCandidates(
				subjects(1).id,
				List((stagings(1).id, stagingTable, 1.0))
			)
		)
	}

	def duplicateCandidates(subjects: List[Subject]): List[DuplicateCandidates] = {
		val stagingTable = "subject_wikidata"
		List(
			DuplicateCandidates(
				subjects.head.id,
				List((subjects(1).id, stagingTable, 0.9784615384615384), (subjects(4).id, stagingTable, 0.5761904761904763))),
			DuplicateCandidates(
				subjects(1).id,
				List((subjects(4).id, stagingTable, 0.5654212454212454))),
			DuplicateCandidates(
				subjects(2).id,
				List((subjects(3).id, stagingTable, 0.9446913580246914))))
	}

	def testConfig(attribute: String = "name"): List[AttributeConfig] = List(
		AttributeConfig(attribute, 1.0, List(SimilarityMeasureConfig(MongeElkan, 0.5), SimilarityMeasureConfig(JaroWinkler, 0.5)))
	)

	def simpleTestConfig: List[AttributeConfig] = List(
		AttributeConfig("name", 1.0, List(SimilarityMeasureConfig(MongeElkan, 1.0)))
	)

	def complexTestConfig: List[AttributeConfig] = List(
		AttributeConfig("name", 0.7, List(SimilarityMeasureConfig(MongeElkan, 0.5), SimilarityMeasureConfig(JaroWinkler, 0.5))),
		AttributeConfig("geo_city", 0.1, List(SimilarityMeasureConfig(Jaccard, 1))),
		AttributeConfig("geo_coords", 0.1, List(SimilarityMeasureConfig(EuclidianDistance, 1))),
		AttributeConfig("gen_income", 0.1, List(SimilarityMeasureConfig(RelativeNumbersSimilarity, 1)))
	)

	def testConfigScore: Double = {
		(MongeElkan.compare("Volkswagen", "Volkswagen AG") + JaroWinkler.compare("Volkswagen", "Volkswagen AG")) * 0.5
	}

	def simpleTestConfigScore: Double = {
		MongeElkan.compare("Volkswagen", "Volkswagen AG")
	}

	def complexTestConfigScore: Double = {
		val name = (MongeElkan.compare("Volkswagen", "Volkswagen AG") * 0.5 + JaroWinkler.compare("Volkswagen", "Volkswagen AG") * 0.5) * 0.7
		val city = Jaccard.compare("Berlin", "Berlin") * 0.1
		val income = RelativeNumbersSimilarity.compare("1234", "12") * 0.1
		name + city + income
	}

	def testCompareScore(subject1: Subject, subject2: Subject, simMeasure: SimilarityMeasure[String], scoreConfig: SimilarityMeasureConfig[String, SimilarityMeasure[String]]): Double = {
		simMeasure.compare(subject1.name.get, subject2.name.get, scoreConfig.scale) * scoreConfig.weight
	}

	def expectedCompareStrategies: List[(List[String], List[String], SimilarityMeasureConfig[String, SimilarityMeasure[String]]) => Double] = List(
			CompareStrategy.singleStringCompare, CompareStrategy.defaultCompare
	)

	def testCompareInput: (List[String], List[String], SimilarityMeasureConfig[String, SimilarityMeasure[String]]) = {
		(
			List("very", "generic", "values"),
			List("even", "more", "values"),
			testConfig().head.scoreConfigs.head
		)
	}

	def cityBlockingScheme: ListBlockingScheme = {
		val scheme = new ListBlockingScheme()
		scheme.setAttributes("geo_city")
		scheme.tag = "GeoBlocking"
		scheme
	}

	def subjects: List[Subject] = List(
		Subject(id = UUID.fromString("974c4495-52fd-445c-b09b-8b769bfb4212"), name = Some("Volkswagen"), properties = Map("geo_city" -> List("Berlin", "Hamburg"), "geo_coords" -> List("52;11"), "gen_income" -> List("1234"))),
		Subject(id = UUID.fromString("f2d98c16-2ac2-4cb7-bd65-4fcb17178060"), name = Some("Audi GmbH"), properties = Map("geo_city" -> List("Berlin"), "geo_coords" -> List("52;13"), "gen_income" -> List("33"))),
		Subject(id = UUID.fromString("c9621786-1cce-45bf-968a-3e8d06e16fda"), name = Some("Porsche"), properties = Map("geo_coords" -> List("52;13"), "gen_sectors" -> List("cars", "music", "games"))),
		Subject(id = UUID.fromString("0ef813fa-55d9-4712-a41d-46a810fca8c9"), name = Some("Ferrari"), properties = Map("geo_coords" -> List("53;14"), "gen_sectors" -> List("games", "cars", "music")))
	)

	def stagings: List[Subject] = List(
		Subject(id = UUID.fromString("4fbc0340-4862-431f-9c28-a508234b8130"), name = Some("Volkswagen AG"), properties = Map("geo_city" -> List("Berlin", "New York"), "gen_income" -> List("12"))),
		Subject(id = UUID.fromString("413c5711-67d0-1151-1077-b000000000b5"), name = Some("Audi GmbH"), properties = Map("geo_city" -> List("New York", "Hamburg"), "geo_coords" -> List("53;14"), "gen_income" -> List("600")))
	)

	def goldStandard(subjects: List[Subject], stagings: List[Subject]): Set[(UUID, UUID)] = Set(
		(subjects.head.id, stagings.head.id),
		(subjects(1).id, stagings.last.id)
	)

	def testBlocks(sc: SparkContext): RDD[Block] = sc.parallelize(Seq(
		Block(key = "Volks", subjects = subjects.take(1), staging = stagings.take(1)),
		Block(key = "Audi ", subjects = List(subjects(1)), staging = List(stagings(1))),
		Block(key = "mix", subjects = subjects, staging = stagings.take(1))
	))

	def flatSubjectBlocks(sc: SparkContext, subjects: List[Subject]): RDD[((String, String), Subject)] = {
		sc.parallelize(List(
			(("Berlin", "GeoBlocking"), subjects.head),
			(("Berlin", "GeoBlocking"), subjects(1)),
			(("Hamburg", "GeoBlocking"), subjects.head),
			(("undefined", "GeoBlocking"), subjects(2)),
			(("undefined", "GeoBlocking"), subjects(3)),
			(("Volks", "SimpleBlocking"), subjects.head),
			(("Audi ", "SimpleBlocking"), subjects(1)),
			(("Porsc", "SimpleBlocking"), subjects(2)),
			(("Ferra", "SimpleBlocking"), subjects(3))
		))
	}

	def flatStagingBlocks(sc: SparkContext, staging: List[Subject]): RDD[((String, String), Subject)] = {
		sc.parallelize(List(
			(("Berlin", "GeoBlocking"), staging.head),
			(("Hamburg", "GeoBlocking"), staging.last),
			(("New York", "GeoBlocking"), staging.head),
			(("New York", "GeoBlocking"), staging.last),
			(("Volks", "SimpleBlocking"), staging.head),
			(("Audi ", "SimpleBlocking"), staging.last)
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
			("SimpleBlocking", Block(id = null, key = "Volks", subjects = subjects.take(1), staging = stagings.take(1))),
			("SimpleBlocking", Block(id = null, key = "Audi ", subjects = List(subjects(1)), staging = List(stagings.last))),
			("SimpleBlocking", Block(id = null, key = "Porsc", subjects = List(subjects(2)))),
			("SimpleBlocking", Block(id = null, key = "Ferra", subjects = List(subjects(3))))
		))
	}

	def groupedAndFilteredBlocks(sc: SparkContext, subjects: List[Subject], stagings: List[Subject]): RDD[(String, Block)] = {
		sc.parallelize(List(
			("GeoBlocking", Block(id = null, key = "Berlin", subjects = subjects.take(2), staging = stagings.take(1))),
			("GeoBlocking", Block(id = null, key = "Hamburg", subjects = subjects.take(1), staging = List(stagings.last))),
			("GeoBlocking", Block(id = null, key = "New York", staging = stagings)),
			("SimpleBlocking", Block(id = null, key = "Volks", subjects = subjects.take(1), staging = stagings.take(1))),
			("SimpleBlocking", Block(id = null, key = "Audi ", subjects = List(subjects(1)), staging = List(stagings.last))),
			("SimpleBlocking", Block(id = null, key = "Porsc", subjects = List(subjects(2)))),
			("SimpleBlocking", Block(id = null, key = "Ferra", subjects = List(subjects(3))))
		))
	}

	def blockEvaluation(sc: SparkContext): RDD[BlockEvaluation] = {
		sc.parallelize(List(
			BlockEvaluation(null, "sum GeoBlocking, SimpleBlocking", Set(
								BlockStats("Hamburg",1,1,0.0),
								BlockStats("undefined",2,0,0.0),
								BlockStats("Berlin",2,1,0.5),
								BlockStats("New York",0,2,0.0),
								BlockStats("Audi ",1,1,1.0),
								BlockStats("Volks",1,1,1.0),
								BlockStats("Porsc",1,0,0.0),
								BlockStats("Ferra",1,0,0.0)
							), Some("Blocking"), 1.0, 8, 5),
			BlockEvaluation(null, "GeoBlocking", Set(
								BlockStats("New York",0,2,0.0),
								BlockStats("Hamburg",1,1,0.0),
								BlockStats("Berlin",2,1,0.5),
								BlockStats("undefined",2,0,0.0)
							), Some("Blocking"), 0.5, 4, 3),
			BlockEvaluation(null, "SimpleBlocking", Set(
								BlockStats("Audi ",1,1,1.0),
								BlockStats("Ferra",1,0,0.0),
								BlockStats("Porsc",1,0,0.0),
								BlockStats("Volks",1,1,1.0)
							), Some("Blocking"), 1.0, 4, 2)))
	}

	def filteredUndefinedBlockEvaluation(sc: SparkContext): RDD[BlockEvaluation] = {
		sc.parallelize(List(
			BlockEvaluation(null, "sum GeoBlocking, SimpleBlocking", Set(
								BlockStats("Hamburg",1,1,0.0),
								BlockStats("Berlin",2,1,0.5),
								BlockStats("New York",0,2,0.0),
								BlockStats("Audi ",1,1,1.0),
								BlockStats("Volks",1,1,1.0),
								BlockStats("Porsc",1,0,0.0),
								BlockStats("Ferra",1,0,0.0)
							), Some("Blocking"), 1.0, 7, 4),
			BlockEvaluation(null, "GeoBlocking", Set(
								BlockStats("New York",0,2,0.0),
								BlockStats("Hamburg",1,1,0.0),
								BlockStats("Berlin",2,1,0.5)
							), Some("Blocking"), 0.5, 3, 3),
			BlockEvaluation(null, "SimpleBlocking", Set(
								BlockStats("Audi ",1,1,1.0),
								BlockStats("Ferra",1,0,0.0),
								BlockStats("Porsc",1,0,0.0),
								BlockStats("Volks",1,1,1.0)
							), Some("Blocking"), 1.0, 4, 2)))
	}

	def blockEvaluationTestSubjects(sc: SparkContext): (RDD[Subject], RDD[Subject]) = {
		val subjectRDD = sc.parallelize(
			List.fill(101)(
				Subject(id = UUID.randomUUID, name = Option("Haushund"))
			) ++ List.fill(5)(
				Subject(id = UUID.randomUUID, name = Option("Nicht Haushund"))
			)
		)
		val stagingRDD = sc.parallelize(
			List.fill(101)(
				Subject(id = UUID.randomUUID, name = Option("Haushund"))
			) ++ List.fill(10)(
				Subject(id = UUID.randomUUID, name = Option("Nicht Haushund"))
			)
		)
		(subjectRDD, stagingRDD)
	}

	def filteredSmallBlockEvaluation(sc: SparkContext): RDD[BlockEvaluation] = {
		sc.parallelize(List(
			BlockEvaluation(null,"sum SimpleBlocking",Set(BlockStats("Haush",101,101,0.0)),Some("Blocking"),0.0,2,10251),
			BlockEvaluation(null,"SimpleBlocking",Set(BlockStats("Haush",101,101,0.0)),Some("Blocking"),0.0,2,10251))
		)
	}

	def blockEvaluationWithComment(sc: SparkContext): RDD[BlockEvaluation] = {
		sc.parallelize(List(
			BlockEvaluation(null, "sum GeoBlocking, SimpleBlocking", Set(
								BlockStats("Hamburg", 1, 1, 0.0),
								BlockStats("undefined", 2, 0, 0.0),
								BlockStats("Berlin", 2, 1, 0.5),
								BlockStats("New York", 0, 2, 0.0),
								BlockStats("Audi ", 1, 1, 1.0),
								BlockStats("Volks", 1, 1, 1.0),
								BlockStats("Porsc", 1, 0, 0.0),
								BlockStats("Ferra", 1, 0, 0.0)
							), Some("Test comment"), 1.0, 8, 4),
			BlockEvaluation(null, "GeoBlocking", Set(
								BlockStats("New York", 0, 2, 0.0),
								BlockStats("Hamburg", 1, 1, 0.0),
								BlockStats("Berlin", 2, 1, 0.5),
								BlockStats("undefined", 2, 0, 0.0)
							), Some("Test comment"), 0.5, 4, 3),
			BlockEvaluation(null, "SimpleBlocking", Set(
								BlockStats("Audi ", 1, 1, 1.0),
								BlockStats("Ferra", 1, 0, 0.0),
								BlockStats("Porsc", 1, 0, 0.0),
								BlockStats("Volks", 1, 1, 1.0)
							), Some("Test comment"), 1.0, 4, 2)
		))
	}

	def emptyBlockEvaluation(sc: SparkContext): RDD[BlockEvaluation] = {
		sc.parallelize(List(
			BlockEvaluation(null,"sum GeoBlocking, SimpleBlocking",Set(),Some("Blocking"),1.0,8,4),
			BlockEvaluation(null,"GeoBlocking",Set(),Some("Blocking"),0.5,4,3),
			BlockEvaluation(null,"SimpleBlocking",Set(),Some("Blocking"),1.0,4,2)
		))
	}

	def emptyBlockEvaluationWithComment(sc: SparkContext): RDD[BlockEvaluation] = {
		sc.parallelize(List(
			BlockEvaluation(null,"sum GeoBlocking, SimpleBlocking",Set(),Some("Test comment"),1.0,8,4),
			BlockEvaluation(null,"GeoBlocking",Set(),Some("Test comment"),0.5,4,3),
			BlockEvaluation(null,"SimpleBlocking",Set(),Some("Test comment"),1.0,4,2)
		))
	}

	def simpleBlockingScheme: List[List[String]] = List(List("Audi "), List("Volks"), List("Ferra"), List("Porsc"))
	def lastLettersBlockingScheme: List[List[String]] = List(List(" GmbH"), List("wagen"), List("rrari"), List("rsche"), List("undefined"))
	def listBlockingScheme: List[List[String]] = List(List("Berlin", "Hamburg", "1234"), List("Berlin", "33"), List("undefined"))
	def geoCoordsBlockingScheme: List[List[String]] = List(List("52;11"), List("52;13"), List("53;14"), List("undefined"))
	def randomBlockingScheme: List[List[String]] = List(List("-235"), List("754"), List("446"), List("-7"))
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
		FeatureEntry(subject = Subject(id = idList.head), staging = Subject(id = idList(1), name = Option("Audi AG")), correct = true),
		FeatureEntry(subject = Subject(id = idList(2)), staging = Subject(id = idList(3), name = Option("Volkswagen AG")), correct = true),
		FeatureEntry(subject = Subject(id = idList(4)), staging = Subject(id = idList(5), name = Option("Porsche AG")))
	))

	def requiredSettings: List[String] = {
		List("keyspaceSubjectTable", "subjectTable", "keyspaceStagingTable", "stagingTable", "keyspaceStatsTable", "statsTable")
	}


	def distinctDuplicateCandidates(sc: SparkContext): RDD[(Subject, Subject, Double)] = {
		sc.parallelize(List(
			(subjects.head, stagings.head, 0.0),
			(subjects(1), stagings(1), 0.0),
			(subjects(1), stagings.head, 0.0),
			(subjects(2), stagings.head, 0.0),
			(subjects(3), stagings.head, 0.0)
		))
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
			Subject(id = idList.head, name = Option("Audi ")),
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
