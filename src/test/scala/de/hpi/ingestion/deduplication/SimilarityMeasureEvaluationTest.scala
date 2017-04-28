package de.hpi.ingestion.deduplication

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}


class SimilarityMeasureEvaluationTest extends FlatSpec
	with Matchers
	with SharedSparkContext
	with RDDComparisons
{
	"generatePredictionAndLabels" should "find all TRUE POSITIVES" in {
		val test = TestData.testData(sc)
		val training = TestData.trainingData(sc)
		val truePositives = SimilarityMeasureEvaluation
			.generatePredictionAndLabels(training, test)
			.filter { case (prediction, label) =>
				prediction > 0 && label == 1
			}
		val expected = TestData.truePositives(sc)
		assertRDDEquals(expected, truePositives)
	}

	it should "find all FALSE POSITIVES" in {
		val test = TestData.testData(sc)
		val training = TestData.trainingData(sc)
		val falsePositives = SimilarityMeasureEvaluation
			.generatePredictionAndLabels(training, test)
			.filter { case (prediction, label) =>
				prediction > 0 && label == 0
			}
		val expected = TestData.falsePositives(sc)
		assertRDDEquals(expected, falsePositives)
	}

	it should "find all FALSE NEGATIVES" in {
		val test = TestData.testData(sc)
		val training = TestData.trainingData(sc)
		val falseNegatives = SimilarityMeasureEvaluation
			.generatePredictionAndLabels(training, test)
			.filter { case (prediction, label) =>
				prediction == 0 && label == 1
			}
		val expected = TestData.falseNegatives(sc)
		assertRDDEquals(expected, falseNegatives)
	}

	"generateStats" should "create a correct computation of precision + recall" in {
		val testData = TestData.testData(sc)
		val trainingData = TestData.trainingData(sc)

		val predictionAndLabels = SimilarityMeasureEvaluation.generatePredictionAndLabels(trainingData, testData)
		val computed = SimilarityMeasureEvaluation.generateStats(predictionAndLabels)
		val expected = TestData.precisionRecallResults(sc)

		assertRDDEquals(computed, expected)
	}
}
