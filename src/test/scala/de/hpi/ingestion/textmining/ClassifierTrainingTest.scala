package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.deduplication.models.SimilarityMeasureStats
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD

class ClassifierTrainingTest extends FlatSpec with Matchers with SharedSparkContext {

	"Run" should "return statistics and a possible model" in {
		val oldFunction = ClassifierTraining.trainingFunction

		ClassifierTraining.trainingFunction = TestData.crossValidationMethod
		val input = List(sc.parallelize(TestData.classifierFeatureEntries())).toAnyRDD()
		val List(stats, model) = ClassifierTraining.run(input, sc)
		val simStats = stats.asInstanceOf[RDD[SimilarityMeasureStats]].first.copy(id = null)
		val expectedStats = TestData.simMeasureStats()
		simStats shouldEqual expectedStats

		val modelOpt = model.asInstanceOf[RDD[Option[RandomForestModel]]].first
		modelOpt should not be empty
		modelOpt.isInstanceOf[Option[RandomForestModel]] shouldBe true

		ClassifierTraining.trainingFunction = oldFunction
	}

	"Statistics" should "be calculated" in {
		val data = sc.parallelize(TestData.labeledPredictions())
		val statistics = ClassifierTraining.calculateStatistics(data, 0.5)
		val expectedStats = TestData.predictionStatistics()
		statistics shouldEqual expectedStats
	}

	they should "be averaged" in {
		val statistics = TestData.statisticTuples()
		val normalAvg = ClassifierTraining.averageStatistics(statistics)
		val expectedAvg = TestData.averagedStatisticTuples()
		normalAvg shouldEqual expectedAvg

		val nanAvg = ClassifierTraining.averageStatistics(Nil)
		nanAvg.threshold shouldBe 1.0
		nanAvg.precision.isNaN shouldBe true
		nanAvg.recall.isNaN shouldBe true
		nanAvg.fscore.isNaN shouldBe true
	}

	"Naive Bayes model" should "be returned" in {
		val data = sc.parallelize(TestData.classifierFeatureEntries().map(_.labeledPoint()))
		val model = ClassifierTraining.trainNaiveBayes(data)
		model.isInstanceOf[NaiveBayesModel] shouldBe true
	}

	"Random Forest model" should "be returned" in {
		val data = sc.parallelize(TestData.classifierFeatureEntries().map(_.labeledPoint()))
		val model = ClassifierTraining.trainRandomForest(data, 1, 2, 2)
		model.isInstanceOf[RandomForestModel] shouldBe true
	}

	"Cross validation" should "return statistics and models" in {
		val data = sc.parallelize(TestData.extendedClassifierFeatureEntries(7).map(_.labeledPoint()))
		val (stats, models) = ClassifierTraining.crossValidate(data, 2, 1, 2, 2)
		stats should not be empty
		models should not be empty
	}

	it should "average the statistics and return only one model" in {
		val data = sc.parallelize(TestData.extendedClassifierFeatureEntries(7).map(_.labeledPoint()))
		val (avgStats, model) = ClassifierTraining.crossValidateAndEvaluate(data, 3, 1, 2, 2)
		avgStats.comment should contain ("Random Forest Cross Validation")
		avgStats.data should not be empty
		model shouldBe defined
	}
}
