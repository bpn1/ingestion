package de.hpi.ingestion.dataimport.dbpedia

import org.scalatest.{FlatSpec, Matchers}

class DBpediaNormalizeStrategyUnitTest extends FlatSpec with Matchers {
	def expectedNormalizeStrategies: List[List[String] => List[String]] = List(
		DBpediaNormalizeStrategy.normalizeEmployees,
		DBpediaNormalizeStrategy.normalizeCountry,
		DBpediaNormalizeStrategy.normalizeNothing
	)

	"apply" should "decide, which strategy should be used regarding the input attribute" in {
		val input = List(
			"5500^^xsd:integer", "100^^xsd:nonNegativeInteger", "über 1000@de .",	// normalizeEmployees
			"Deutschland@de .", "dbpedia-de:England", // normalizeCountry
			"I don't fit anywhere :("	// normalizeNothing
		)
		val strategy = List(
			DBpediaNormalizeStrategy.apply("gen_employees"),
			DBpediaNormalizeStrategy.apply("geo_country"),
			DBpediaNormalizeStrategy.apply("something_else")
		)
		val output = strategy.map(_(input))
		val expected = expectedNormalizeStrategies.map(_(input))
		output shouldEqual expected
	}

	/*"compare" should "return the score of the similarity of two strings" in {
		val deduplication = TestData.defaultDeduplication
		deduplication.config = TestData.testConfig()
		val subjects = TestData.testSubjects
		val subject1 = subjects.head
		val subject2 = subjects(1)
		for {
			(attribute, scoreConfigs) <- deduplication.config
			scoreConfig <- scoreConfigs
		}{
			val score = scoreConfig.compare(subject1.get(attribute).head, subject2.get(attribute).head)
			val expected = TestData.testCompareScore(
				subject1,
				subject2,
				scoreConfig.similarityMeasure,
				scoreConfig
			)
			score shouldEqual expected
		}
	}

	"simpleStringCompare" should "only compare the first element in a list" in {
		val deduplication = TestData.defaultDeduplication
		deduplication.config = TestData.testConfig()
		val subjects = TestData.testSubjects
		val subject1 = subjects.head
		val subject2 = subjects(1)
		for {
			(attribute, scoreConfigs) <- deduplication.config
			scoreConfig <- scoreConfigs
		}{
			val score = CompareStrategy.singleStringCompare(
				subject1.get(attribute),
				subject2.get(attribute),
				scoreConfig
			)
			val expected = TestData.testCompareScore(
				subject1,
				subject2,
				scoreConfig.similarityMeasure,
				scoreConfig
			)
			score shouldEqual expected
		}
	}

	"coordinatesCompare" should "compare the input lists as coordinate values" in {
		val coordinates = List(List("1.5", "1", "10", "10"), List("1.1", "1", "10", "10"))
		val attribute = "geo_coords"
		val feature = ScoreConfig[String, SimilarityMeasure[String]](EuclidianDistance, 1)
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
		for {
			(attribute, scoreConfigs) <- deduplication.config
			scoreConfig <- scoreConfigs
		}{
			val score = BigDecimal(CompareStrategy.defaultCompare(
				subject1.get(attribute),
				subject2.get(attribute),
				scoreConfig
			)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
			val expected = 1.0 * scoreConfig.weight
			score shouldEqual expected
		}
	}
	*/
}
