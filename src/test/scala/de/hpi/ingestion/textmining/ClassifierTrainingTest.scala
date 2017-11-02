/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.deduplication.models.SimilarityMeasureStats
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class ClassifierTrainingTest extends FlatSpec with Matchers with SharedSparkContext {

    "Run" should "return statistics and a possible model" in {
        val job = new ClassifierTraining
        job.featureEntries = sc.parallelize(TestData.extendedClassifierFeatureEntries(10))
        job.run(sc)
        val simStats = job.similarityMeasureStats.first.copy(id = null, comment = None, yaxis = None)
        val expectedStats = TestData.simMeasureStats()
        simStats shouldEqual expectedStats
        job.model should not be empty
    }

    "Statistics" should "be calculated" in {
        val data = sc.parallelize(TestData.labeledPredictions())
        val statistics = ClassifierTraining.calculateStatistics(data, 0.5)
        val expectedStats = TestData.predictionStatistics()
        statistics shouldEqual expectedStats
    }

    they should "be NaN if there are no values to calculate them" in {
        val data = sc.parallelize(TestData.badLabeledPredictions())
        val statistics = ClassifierTraining.calculateStatistics(data, 0.5)
        statistics.precision.isNaN shouldBe true
        statistics.recall.isNaN shouldBe true
        statistics.fscore.isNaN shouldBe true
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

    "Cross validation" should "return statistics and models" in {
        val session = SparkSession.builder().getOrCreate()
        val data = sc.parallelize(TestData.extendedClassifierFeatureEntriesWithGrouping(7))
        val classifier = ClassifierTraining.randomForestDFModel(1, 8, 1)
        val (stats, models) = ClassifierTraining.crossValidate(data, 2, classifier, session)
        stats should not be empty
        models should not be empty
    }

    it should "average the statistics and return only one model" in {
        val session = SparkSession.builder().getOrCreate()
        val data = sc.parallelize(TestData.extendedClassifierFeatureEntries(7))
        val classifier = ClassifierTraining.randomForestDFModel(1, 8, 1)
        val (avgStats, model) = ClassifierTraining.crossValidateAndEvaluate(data, 2, classifier, session)
        avgStats.data should not be empty
        model shouldBe defined
    }

    "Prediction and labels" should "be transformed into keys" in {
        val predictions = TestData.predictionData()
        val predictionKeys = predictions.map((ClassifierTraining.trueOrFalsePrediction _).tupled)
        val expectedKeys = TestData.predictionKeys()
        predictionKeys shouldEqual expectedKeys
    }
}
