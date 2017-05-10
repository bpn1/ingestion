package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.mllib.classification.NaiveBayesModel

class ClassifierTrainingTest extends FlatSpec with Matchers with SharedSparkContext {

	"Run" should "return a Naive Bayes model" in {
		val input = List(sc.parallelize(TestData.classifierFeatureEntries())).toAnyRDD()
		val modelList = ClassifierTraining.run(input, sc).fromAnyRDD[NaiveBayesModel]().head.collect.toList
		modelList should not be empty
	}

	"Statistics" should "be calculated" in {
		val data = sc.parallelize(TestData.labeledPredictions())
		val statistics = ClassifierTraining.calculateStatistics(data, 0.5)
		val expectedStats = TestData.predictionStatistics()
		statistics shouldEqual expectedStats
	}

	they should "be formatted" in {
		val statistics = TestData.predictionStatistics()
		val formattedStats = ClassifierTraining.formatStatistics(statistics)
		val expectedString = TestData.formattedPredictionStatistics()
		formattedStats shouldEqual expectedString
	}
}
