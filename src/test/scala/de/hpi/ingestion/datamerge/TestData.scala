package de.hpi.ingestion.datamerge

import java.util.UUID

import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.models.DuplicateCandidates
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._

object TestData {
	// scalastyle:off line.size.limit
	val subjectIds = List.fill(10)(UUID.randomUUID())
	def testVersion(sc: SparkContext): Version = Version("SomeTestApp", Nil, sc)
	def testSubjects: List[Subject] = List(
		Subject(
			id = subjectIds.head,
			name = Some("Volkswagen"),
			properties = Map(
				"geo_city" -> List("Berlin", "Hamburg"),
				"geo_coords" -> List("52","11"),
				"gen_employees" -> List("1234")
			)
		),
		Subject(
			id = subjectIds(1),
			name = Some("Volkswagen AG"),
			properties = Map(
				"geo_city" -> List("Berlin", "New York"),
				"gen_employees" -> List("12")
			)
		),
		Subject(
			id = subjectIds(2),
			name = Some("Volkswagen KG"),
			properties = Map("geo_coords" -> List("52", "12"))
		)
	)
	def masterNode: Subject = testSubjects.head
	def duplicates: DuplicateCandidates = DuplicateCandidates(
		testSubjects.head.id,
		candidates = testSubjects.tail.map((_, "source", 0.6))
	)
	def expectedProperties: Map[String, List[String]] = Map(
		"geo_city" -> List("Berlin", "Hamburg", "New York"),
		"geo_coords" -> List("52", "11", "52", "12"),
		"gen_employees" -> List("1234", "12")
	)
	def expectedRelations: Map[UUID, Map[String, String]] = Map(
		subjectIds(1) -> Map("type" -> "isDuplicate", "confidence" -> "0.6"),
		subjectIds.last -> Map("type" -> "isDuplicate", "confidence" -> "0.6")
	)

	def subjects: List[Subject] = List(
		Subject(id = subjectIds.head, name = Some("Audi"), properties = Map("geo_city" -> List("München"))),
		Subject(id = subjectIds(1), name = Some("Mercedes"), properties = Map("geo_postal" -> List("10666"))),
		Subject(id = subjectIds(2), name = Some("BMW")),
		Subject(id = subjectIds(3), name = Some("Audi"), properties = Map("geo_city" -> List("Berlin"), "type" -> List("Auto"))),
		Subject(id = subjectIds(4), name = Some("Audi AG"), properties = Map("geo_postal" -> List("10888"))),
		Subject(id = subjectIds(5), name = Some("Mercedes Benz"), properties = Map("geo_coords" -> List("1", "1"))),
		Subject(id = subjectIds(6), name = Some("Mercedes B."), properties = Map("geo_coords" -> List("1", "2", "3", "4"))),
		Subject(id = subjectIds(7), name = Some("Volkswagen"))
	)
	def subjectRDD(sc: SparkContext): RDD[Subject] = sc.parallelize(List(
		subjects.head, subjects(1), subjects(2)
	))
	def stagingRDD(sc: SparkContext): RDD[Subject] = sc.parallelize(List(
		subjects(3), subjects(4), subjects(5), subjects(6), subjects(7)
	))
	def duplicateCandidatesRDD(sc: SparkContext): RDD[DuplicateCandidates] = sc.parallelize(List(
		DuplicateCandidates(
			subjects.head.id,
			List(
				(subjects(3), "source", 0.6),
				(subjects(4), "source", 0.6)
			)
		),
		DuplicateCandidates(
			subjects(1).id,
			List(
				(subjects(5), "source", 0.6),
				(subjects(6), "source", 0.6)
			)
		)
	))
	def inputRDD(sc: SparkContext): List[RDD[Any]] = List(
		subjectRDD(sc),
		stagingRDD(sc),
		duplicateCandidatesRDD(sc)
	).toAnyRDD()
	def outputRDD(sc: SparkContext): List[RDD[Any]] = List(sc.parallelize(List(
		Subject(id = subjectIds.head, name = Some("Audi"), properties = Map(
			"geo_city" -> List("München", "Berlin"), "geo_postal" -> List("10888")
		), relations = Map(
			subjectIds(3) -> Map("type" -> "Duplicate", "confidence" -> "0.5")
		)),
		Subject(id = subjectIds(1)),
		Subject(id = subjectIds(3)),
		Subject(id = subjectIds(4)),
		Subject(id = subjectIds(5)),
		Subject(id = subjectIds(6)),
		subjects.last
	))).toAnyRDD()
	// scalastyle:on line.size.limit
}
