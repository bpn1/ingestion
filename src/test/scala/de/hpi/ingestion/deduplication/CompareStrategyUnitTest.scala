package de.hpi.ingestion.deduplication

import de.hpi.ingestion.deduplication.models.ScoreConfig
import de.hpi.ingestion.deduplication.similarity.{EuclidianDistance, SimilarityMeasure}
import org.scalatest.{FlatSpec, Matchers}

import scala.math.BigDecimal

class CompareStrategyUnitTest extends FlatSpec with Matchers {
	"compare" should "return the score of the similarity of two strings" in {
		val deduplication = TestData.defaultDeduplication
		deduplication.config = TestData.testConfig()
		val subjects = TestData.testSubjects
		val subject1 = subjects(0)
		val subject2 = subjects(1)
		deduplication.config.map { feature =>
			(feature.compare(
				subject1.get(feature.key).head,
				subject2.get(feature.key).head
			), TestData.testCompareScore(subject1, subject2, feature.similarityMeasure, feature))
		}.foreach { case (score, expected) => score shouldEqual expected }
	}

	"simpleStringCompare" should "only compare the first element in a list" in {
		val deduplication = TestData.defaultDeduplication
		deduplication.config = TestData.testConfig()
		val subjects = TestData.testSubjects
		val subject1 = subjects(0)
		val subject2 = subjects(1)
		deduplication.config.map { feature =>
			(CompareStrategy.singleStringCompare(
				subject1.get(feature.key),
				subject2.get(feature.key),
				feature
			), TestData.testCompareScore(subject1, subject2, feature.similarityMeasure, feature))
		}.foreach {case (score, expected) => score shouldEqual expected}
	}

	"coordinatesCompare" should "compare the input lists as coordinate values" in {
		val coordinates = List(List("1.5", "1", "10", "10"), List("1.1", "1", "10", "10"))
		val attribute = "geo_coords"
		val feature = ScoreConfig[String, SimilarityMeasure[String]](attribute, EuclidianDistance, 1)
		val score = CompareStrategy.coordinatesCompare(
			coordinates.head,
			coordinates.last,
			feature
		)
		val expected = 0.75
		score shouldEqual expected
	}

	"defaultCompare" should "compare each element from a list with each element from another" in {
		val deduplication = TestData.defaultDeduplication
		deduplication.config = TestData.testConfig("gen_sectors")
		val subjects = TestData.testSubjects
		val subject1 = subjects(4)
		val subject2 = subjects(5)
		deduplication.config.foreach { feature =>
			val score = BigDecimal(CompareStrategy.defaultCompare(
				subject1.get(feature.key),
				subject2.get(feature.key),
				feature
			)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble	// rounds the value

			val expected = 1.0 * feature.weight
			score shouldEqual expected
		}
	}

	"apply" should "decide, which strategy should be used regarding the input attribute" in {
		val inputValues = TestData.testCompareInput
		val strategies = List(
			CompareStrategy.apply("geo_city"),
			CompareStrategy.apply("geo_coords"),
			CompareStrategy.apply("gen_income")
		)
		val output = strategies.map(_.tupled(inputValues))
		val expected = TestData.expectedCompareStrategies.map(_.tupled(inputValues))
		output shouldEqual expected
	}
}
