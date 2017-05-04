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
	def masterNode: Subject = subjects.head
	def duplicates: DuplicateCandidates = DuplicateCandidates(
		subjects.head.id,
		candidates = List(subjects(3), subjects(4)).map((_, "source", 0.6))
	)
	def expectedProperties: Map[String, List[String]] = Map(
		"geo_city" -> List("München", "Berlin"),
		"geo_postal" -> List("10888")
	)
	def expectedRelations: Map[UUID, Map[String, String]] = Map(
		subjectIds(3) -> Map("type" -> "isDuplicate", "confidence" -> "0.6"),
		subjectIds(4) -> Map("type" -> "isDuplicate", "confidence" -> "0.6")
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
	def inputRDD(sc: SparkContext): List[RDD[Any]] = {
		List(subjectRDD(sc), stagingRDD(sc)).toAnyRDD() ::: List(duplicateCandidatesRDD(sc)).toAnyRDD()
	}
	def outputRDD(sc: SparkContext): List[RDD[Any]] = {
		val relation = Map("type" -> "Duplicate", "confidence" -> "0.5")
		List(sc.parallelize(List(
			Subject(
				id = subjectIds.head,
				name = Some("Audi"),
				properties = Map("geo_city" -> List("München", "Berlin"), "geo_postal" -> List("10888")),
				relations = Map(subjectIds(3) -> relation, subjectIds(4) -> relation)
			),
			Subject(
				id = subjectIds(1),
				name = Some("Mercedes"),
				properties = Map("geo_postal" -> List("10666"), "geo_coords" -> List("1", "1", "1", "2", "3", "4")),
				relations = Map(subjectIds(5) -> relation, subjectIds(6) -> relation)
			),
			Subject(
				id = subjectIds(3),
				name = Some("Audi"),
				properties = Map("geo_city" -> List("Berlin"), "type" -> List("Auto")),
				relations = Map(subjectIds.head -> relation)
			),
			Subject(
				id = subjectIds(4),
				name = Some("Audi AG"),
				properties = Map("geo_postal" -> List("10888")),
				relations = Map(subjectIds.head -> relation)
			),
			Subject(
				id = subjectIds(5),
				name = Some("Mercedes Benz"),
				properties = Map("geo_coords" -> List("1", "1")),
				relations = Map(subjectIds(1) -> relation)
			),
			Subject(
				id = subjectIds(6),
				name = Some("Mercedes B."),
				properties = Map("geo_coords" -> List("1", "2", "3", "4")),
				relations = Map(subjectIds(1) -> relation)
			),
			subjects.last
		))).toAnyRDD()
	}
	// scalastyle:on line.size.limit
}
