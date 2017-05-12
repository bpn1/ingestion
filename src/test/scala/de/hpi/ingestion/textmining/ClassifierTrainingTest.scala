package de.hpi.ingestion.textmining
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD

class ClassifierTrainingTest extends FlatSpec with Matchers with SharedSparkContext {

	"Run" should "return a Naive Bayes model" in {
		val oldFunction = ClassifierTraining.trainingFunction

//		val input = List(sc.parallelize(TestData.classifierFeatureEntries())).toAnyRDD()
//		val modelList = ClassifierTraining.run(input, sc).last.asInstanceOf[RDD[RandomForestModel]].collect
//		modelList should not be empty
		assert(false)

		ClassifierTraining.trainingFunction = oldFunction
	}

	"Statistics" should "be calculated" in {
		val data = sc.parallelize(TestData.labeledPredictions())
		val statistics = ClassifierTraining.calculateStatistics(data, 0.5)
		val expectedStats = TestData.predictionStatistics()
		statistics shouldEqual expectedStats
	}

	they should "be formatted" in {
		val statistics = ClassifierTraining.formatStatistics(TestData.predictionStatistics())
		val expectedString = TestData.formattedPredictionStatistics()
		statistics shouldEqual expectedString
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

	"Cross validation" should "be correct" in {
		assert(false)
	}
}
