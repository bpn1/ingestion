package de.hpi.ingestion.deduplication.similarity

import de.hpi.ingestion.deduplication.TestData
import org.scalatest.{FlatSpec, Matchers}

class EuclidianDistanceUnitTest extends FlatSpec with Matchers {
	"computeDistance" should "compute the distnace between two points in terms of kilometers" in {
		val subjects = TestData.testSubjects
		val distance = EuclidianDistance.computeDistance(
			subjects(4).properties("geo_coords").head.toList(0).toDouble,
			subjects(4).properties("geo_coords").head.toList(1).toDouble,
			subjects(5).properties("geo_coords").head.toList(0).toDouble,
			subjects(5).properties("geo_coords").head.toList(1).toDouble
		)

		val expected = 101.2406771798861

		distance shouldEqual expected
	}

	"turnDistanceIntoScore" should "compute the right score for given distances and scale factors" in {
		val distances = List(0.0, 1.0, 5.1, 21.0, 312.0)
		val testScale1 = 1
		val testScale2 = 2
		val scores = distances.map { value =>
			(
				EuclidianDistance.turnDistanceIntoScore(value, testScale1),
				EuclidianDistance.turnDistanceIntoScore(value, testScale2)
			)
		}
		val expectedScores = List(
			(1.0, 1.0),
			(1.0, 1.0),
			(0.75, 1.0),
			(0.5, 0.75),
			(0.0, 0.0)
		)
		(scores, expectedScores).zipped.foreach { case (score, expected) =>
			score shouldEqual expected
		}

	}

	"compare" should "compute correct score for two given points" in {
		val subjects = TestData.testSubjects
		val geoPoints = List(
			subjects.head.properties("geo_coords").mkString(";"),
			subjects(2).properties("geo_coords").mkString(";"),
			subjects(3).properties("geo_coords").mkString(";")
		)

		val scores = List(
			EuclidianDistance.compare(geoPoints.head, geoPoints.head),
			EuclidianDistance.compare(geoPoints.head, geoPoints(1)),
			EuclidianDistance.compare(geoPoints.head, geoPoints(2))
		)
		val expectedScores = List(1.0, 0.5, 0.0)
		(scores, expectedScores).zipped.foreach { case (score, expected) =>
			score shouldEqual expected
		}
	}
}
