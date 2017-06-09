package de.hpi.ingestion.deduplication.similarity

import org.scalatest.{FlatSpec, Matchers}

class EuclidianDistanceUnitTest extends FlatSpec with Matchers {
	"computeDistance" should "compute the distnace between two points in terms of kilometers" in {
		val distance = EuclidianDistance.computeDistance(52, 13, 53, 14)
		val expected = 111.70485139435159

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
		val geoPoints = List("52;11", "52;13", "53;14")
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
