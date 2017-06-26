package de.hpi.ingestion.deduplication

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.deduplication.models.FeatureEntry
import de.hpi.ingestion.deduplication.models.config.SimilarityMeasureConfig
import de.hpi.ingestion.deduplication.similarity.{ExactMatchString, SimilarityMeasure}
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.scalatest.{FlatSpec, Matchers}

class FeatureCalculationTest extends FlatSpec with Matchers with SharedSparkContext with RDDComparisons {
	"compare" should "calculate a similarity score of two subjects from a given config" in {
		val config = SimilarityMeasureConfig[String, SimilarityMeasure[String]](ExactMatchString, 1.0)
		val attribute = "geo_city"
		val subject = TestData.subjects.head.get(attribute)
		val staging = TestData.stagings.head.get(attribute)
		val expected = CompareStrategy(attribute)(subject, staging, config)
		val result = FeatureCalculation.compare(attribute, subject, staging, config)

		result shouldEqual expected
	}

	it should "return 0.0 if one of the given subjects doesn't hold a property" in {
		val config = SimilarityMeasureConfig[String, SimilarityMeasure[String]](ExactMatchString, 1.0)
		val attribute = "geo_city"
		val subject = TestData.subjects.head.get(attribute)
		val staging = TestData.subjects.last.get(attribute)
		val result = FeatureCalculation.compare(attribute, subject, staging, config)

		result shouldEqual 0.0
	}

	"createFeature" should "create a FeatureEntry of a given subject pair" in {
		val config = TestData.testConfig("geo_city")
		val subject = TestData.subjects.head
		val staging = TestData.stagings.head
		val feature = FeatureCalculation.createFeature(subject, staging, config)
		val expected = FeatureEntry(feature.id, subject, staging, Map("geo_city" -> List(0.25, 0.3556547619047619)))

		feature shouldEqual expected
	}

	"labelFeature" should "label a FeatureEntries by a given Gold-Standard" in {
		val features = TestData.features(sc)
		val goldStandard = TestData.goldStandard(sc)
		val labeledFeatures = FeatureCalculation.labelFeature(features, goldStandard).map(_.copy(id = null))
		val expected = TestData.labeledFeatures(sc).map(_.copy(id = null))
		assertRDDEquals(expected, labeledFeatures)
	}

	"Features" should "be calculated" in {
		val dbpedia = sc.parallelize(TestData.dbpediaEntries)
		val wikidata = sc.parallelize(TestData.wikidataEntries)
		val goldStandard = TestData.goldStandard(sc)
		val input = List(dbpedia, wikidata).toAnyRDD() ++ List(goldStandard).toAnyRDD()
		val result = FeatureCalculation.run(input, sc).fromAnyRDD[FeatureEntry]().head.map(_.copy(id = null))
		val expectedFeatureEntries = TestData.labeledFeatures(sc).map(_.copy(id = null))
		assertRDDEquals(expectedFeatureEntries, result)
	}
}
