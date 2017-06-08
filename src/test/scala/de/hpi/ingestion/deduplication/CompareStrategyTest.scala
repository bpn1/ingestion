package de.hpi.ingestion.deduplication

import scala.math.BigDecimal
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.deduplication.models.config.AttributeConfig

class CompareStrategyTest extends FlatSpec with Matchers {
	"compare" should "return the score of the similarity of two strings" in {
		val config = TestData.testConfig()
		val subject = TestData.subjects.head
		val staging = TestData.stagings.head
		for {AttributeConfig(attribute, weight, scoreConfigs) <- config; scoreConfig <- scoreConfigs}{
			val score = scoreConfig.compare(subject.get(attribute).head, staging.get(attribute).head)
			val expected = TestData.testCompareScore(
				subject,
				staging,
				scoreConfig.similarityMeasure,
				scoreConfig
			)
			score shouldEqual expected
		}
	}

	"simpleStringCompare" should "only compare the first element in a list" in {
		val config = TestData.testConfig()
		val subject = TestData.subjects.head
		val staging = TestData.stagings.head
		for {AttributeConfig(attribute, weight, scoreConfigs) <- config; scoreConfig <- scoreConfigs}{
			val score = CompareStrategy.singleStringCompare(
				subject.get(attribute),
				staging.get(attribute),
				scoreConfig
			)
			val expected = TestData.testCompareScore(
				subject,
				staging,
				scoreConfig.similarityMeasure,
				scoreConfig
			)
			score shouldEqual expected
		}
	}

	"defaultCompare" should "compare each element from a list with each element from another" in {
		val config = TestData.testConfig("gen_sectors")
		val subject = TestData.subjects(2)
		val staging = TestData.subjects.last
		for {AttributeConfig(attribute, weight, scoreConfigs) <- config; scoreConfig <- scoreConfigs}{
			val score = BigDecimal(CompareStrategy.defaultCompare(
				subject.get(attribute),
				staging.get(attribute),
				scoreConfig
			)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
			val expected = 1.0 * scoreConfig.weight
			score shouldEqual expected
		}
	}

	"apply" should "decide, which strategy should be used regarding the input attribute" in {
		val inputValues = TestData.testCompareInput
		val strategies = List(
			CompareStrategy.apply("geo_city"),
			CompareStrategy.apply("gen_income")
		)
		val output = strategies.map(_.tupled(inputValues))
		val expected = TestData.expectedCompareStrategies.map(_.tupled(inputValues))
		output shouldEqual expected
	}
}
