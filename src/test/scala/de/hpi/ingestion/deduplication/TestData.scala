package de.hpi.ingestion.deduplication

import java.util.UUID

import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.models.{BlockEvaluation, PrecisionRecallDataTuple, ScoreConfig}
import de.hpi.ingestion.deduplication.similarity.{ExactMatchString, JaroWinkler, MongeElkan, SimilarityMeasure}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object TestData {
	// scalastyle:off line.size.limit
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

	def emptySubject: Subject = Subject()

	def testVersion(sc: SparkContext): Version = Version("SomeTestApp", Nil, sc)

	def parsedConfig: mutable.Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]] = mutable.Map(
		"name" -> List(
			ScoreConfig(similarityMeasure = MongeElkan, weight= 0.8),
			ScoreConfig(similarityMeasure = JaroWinkler, weight= 0.7)
		)
	)

	def testDuplicates(testSubjects: List[Subject]): List[(Subject, Subject, Double)] = List(
		(testSubjects.head, testSubjects(1), 0.7117948717948718),
		(testSubjects.head, testSubjects(4), 0.5761904761904763),
		(testSubjects(1), testSubjects(4), 0.5654212454212454),
		(testSubjects(2), testSubjects(3), 0.9446913580246914)
	)

	def testConfig(key: String = "name"): mutable.Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]] = mutable.Map(
		key -> List(
			ScoreConfig(similarityMeasure = MongeElkan, weight= 0.8),
			ScoreConfig(similarityMeasure = JaroWinkler, weight= 0.7)
		)
	)

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

	def testCompareInput: (List[String], List[String], ScoreConfig[String, SimilarityMeasure[String]]) = (
		List("very", "generic", "values"),
		List("even", "more", "values"),
		testConfig().head._2.head
	)

	def cityBlockingScheme: ListBlockingScheme = {
		val scheme = new ListBlockingScheme()
		scheme.setAttributes("geo_city")
		scheme.tag = "test city blocking scheme"
		scheme
	}

	def blocks(sc: SparkContext, testSubjects: List[Subject]): List[RDD[(String, List[Subject])]] = {
		List(
			sc.parallelize(Seq(
				("Berlin", List(testSubjects.head, testSubjects(1), testSubjects(2))),
				("Hamburg", List(testSubjects.head, testSubjects(3))),
				("New York", List(testSubjects(1), testSubjects(3)))
			)),
			sc.parallelize(Seq(
				("Vol", List(testSubjects.head, testSubjects(1))),
				("Aud", List(testSubjects(2), testSubjects(3))),
				("Por", List(testSubjects(4))),
				("Fer", List(testSubjects(5)))
			))
		)
	}

	def defaultDeduplication: Deduplication = new Deduplication(0.5, "TestDeduplication", List("testSource"))

	def evaluationTestData: List[(Option[String], Map[String, Int])] = {
		List(
			(Option("Test comment_test city blocking scheme"), Map("Berlin" -> 3)),
			(Option("Test comment_test city blocking scheme"), Map("New York" -> 2)),
			(Option("Test comment_test city blocking scheme"), Map("Hamburg" -> 2)),
			(Option("Test comment_SimpleBlockingScheme"), Map("Vol" -> 2)),
			(Option("Test comment_SimpleBlockingScheme"), Map("Aud" -> 2)),
			(Option("Test comment_SimpleBlockingScheme"), Map("Por" -> 1)),
			(Option("Test comment_SimpleBlockingScheme"), Map("Fer" -> 1)))
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
	// scalastyle:on line.size.limit
}
