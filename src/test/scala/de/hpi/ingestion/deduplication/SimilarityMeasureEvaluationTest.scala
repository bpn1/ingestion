package de.hpi.ingestion.deduplication

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.deduplication.models.SimilarityMeasureStats
import de.hpi.ingestion.implicits.CollectionImplicits._

class SimilarityMeasureEvaluationTest extends FlatSpec
	with Matchers
	with SharedSparkContext
	with RDDComparisons
{
	"generateBuckets" should "generate a list of buckets from a given number" in {
		val bucketsList = TestData.bucketsList
		bucketsList.foreach { case (size, buckets) =>
			val generatedBuckets = SimilarityMeasureEvaluation.generateBuckets(size)
			generatedBuckets shouldEqual buckets
		}
	}

	"bucket" should "return the correct bucket for a given score and buckets" in {
		val bucketList = TestData.bucketsList.values
		val scoresList = TestData.testScores
		(bucketList, scoresList).zipped.foreach { case (buckets, scores) =>
			scores.foreach { case (score, expected) =>
				val bucket = SimilarityMeasureEvaluation.bucket(score)(buckets)
				bucket shouldEqual expected
			}
		}
	}

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

	"generatePrecisionRecallData" should "calculate Precision, Recall and F-Score" in {
		val labeledPoints = TestData.labeledPoints(sc)
		implicit val buckets = TestData.bucketsList.values.head
		val data = SimilarityMeasureEvaluation.generatePrecisionRecallData(labeledPoints)
		val expected = TestData.precisionRecallResults
		data shouldEqual expected
	}

	"run" should "calculate Precision, Recall and F-Score" in {
		val training = TestData.trainingCandidates(sc)
		val test = TestData.testData2(sc)
		val input = List(training).toAnyRDD() ::: List(test).toAnyRDD()
		val settings = Map("buckets" -> "5")
		SimilarityMeasureEvaluation.settings = settings
		val data = SimilarityMeasureEvaluation
			.run(input, sc)
			.fromAnyRDD[SimilarityMeasureStats]()
			.head
			.first
			.data
		val expected = TestData.similarityMeasureStats.data
		data shouldEqual expected
	}
}
