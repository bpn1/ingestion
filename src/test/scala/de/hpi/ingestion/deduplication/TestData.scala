package de.hpi.ingestion.deduplication

import java.util.UUID

import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.models.{BlockEvaluation, ScoreConfig}
import de.hpi.ingestion.deduplication.similarity.{ExactMatchString, JaroWinkler, MongeElkan, SimilarityMeasure}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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

	def trainigsData(sc: SparkContext): RDD[((UUID, UUID), Double)] = {
		sc.parallelize(Seq(
			((idList.head, idList(1)), 0.7),
			((idList(2), idList(3)), 0.8),
			((idList(4), idList(7)), 0.5),
			((idList(6), idList(5)), 0.6)
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

	def propertyKeyDBPedia(sc: SparkContext): RDD[(String, Subject)] = {
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

	def joinedDBPediaWikiData(sc: SparkContext): RDD[(UUID, UUID)] = {
		sc.parallelize(Seq(
			(dbpediaList.head.id, wikidataList.head.id),
			(dbpediaList(1).id, wikidataList(1).id),
			(dbpediaList(2).id, wikidataList(2).id)
		))
	}

	def testSubjects: List[Subject] = List(
		Subject(name = Some("Volkswagen"), properties = Map("geo_city" -> List("Berlin"), "geo_coords" -> List("52","11"), "gen_income" -> List("1234"))),
		Subject(name = Some("Volkswagen AG"), properties = Map("geo_city" -> List("Berlin"), "gen_income" -> List("12"))),
		Subject(name = Some("Audi GmbH"), properties = Map("geo_city" -> List("Berlin"), "geo_coords" -> List("52","13"), "gen_income" -> List("33"))),
		Subject(name = Some("Audy GmbH"), properties = Map("geo_city" -> List("New York"), "geo_coords" -> List("53","14"), "gen_income" -> List("600"))),
		Subject(name = Some("Porsche"), properties = Map("geo_coords" -> List("52","13"))),
		Subject(name = Some("Ferrari"), properties = Map("geo_coords" -> List("53","14")))
	)

	def testVersion(sc: SparkContext): Version = Version("SomeTestApp", Nil, sc)

	def testConfig: List[ScoreConfig[String, SimilarityMeasure[String]]] = List(
		ScoreConfig(key = "name", similarityMeasure = MongeElkan, weight= 0.8),
		ScoreConfig(key = "name", similarityMeasure = JaroWinkler, weight= 0.7),
		ScoreConfig(key = "name", similarityMeasure = ExactMatchString, weight= 0.2)
	)

	def testSubjectScore(subject1: Subject, subject2: Subject): Double = List(
			MongeElkan.compare(subject1.name.get, subject2.name.get) * 0.8,
			JaroWinkler.compare(subject1.name.get, subject2.name.get) * 0.7,
			ExactMatchString.compare(subject1.name.get, subject2.name.get) * 0.2
	).sum / 3

	def cityBlockingScheme: ListBlockingScheme = {
		val scheme = new ListBlockingScheme()
		scheme.setAttributes("geo_city")
		scheme
	}

	def cityBlock(testSubjects: List[Subject]): List[(List[List[String]], List[Subject])] = List(
		(List(List("Berlin")),  List(testSubjects.head, testSubjects(1), testSubjects(2))),
		(List(List("New York")), List(testSubjects(3))),
		(List(List("undefined")), List(testSubjects(4), testSubjects(5)))
	)

	def defaultDeduplication: Deduplication = new Deduplication(0.5, "TestDeduplication", List("testSource"))

	def evaluationTestData: BlockEvaluation = BlockEvaluation(data = Map(List("Berlin") -> 3,  List("New York") -> 1, List("undefined") -> 2), comment = Option("Test comment"))
	// scalastyle:on line.size.limit
}
