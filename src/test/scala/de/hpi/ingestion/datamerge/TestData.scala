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

	def subjects(sc: SparkContext): RDD[Subject] = sc.parallelize(List(
		Subject(id = subjectIds.head),
		Subject(id = subjectIds(1)),
		Subject(id = subjectIds(2))
	))
	def staging(sc: SparkContext): RDD[Subject] = sc.parallelize(List(
		Subject(id = subjectIds(3)),
		Subject(id = subjectIds(4)),
		Subject(id = subjectIds(5)),
		Subject(id = subjectIds(6)),
		Subject(id = subjectIds(7))
	))
	def duplicateCandidates(sc: SparkContext): RDD[DuplicateCandidates] = sc.parallelize(List(
		DuplicateCandidates(
			subjectIds.head,
			List(
				(Subject(id = subjectIds(3)), "source", 0.6),
				(Subject(id = subjectIds(4)), "source", 0.6)
			)
		),
		DuplicateCandidates(
			subjectIds(1),
			List(
				(Subject(id = subjectIds(5)), "source", 0.6),
				(Subject(id = subjectIds(6)), "source", 0.6)
			)
		)
	))
	def inputRDD(sc: SparkContext): List[RDD[Any]] = List(subjects(sc), staging(sc), duplicateCandidates(sc)).toAnyRDD()
	//def outputRDD(sc: SparkContext): List[RDD[Any]] =
	// scalastyle:on line.size.limit
}
