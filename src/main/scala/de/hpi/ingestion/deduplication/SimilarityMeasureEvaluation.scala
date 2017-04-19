package de.hpi.ingestion.deduplication

import java.util.UUID
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

object SimilarityMeasureEvaluation {
	val appName = "SimilarityMeasureEvaluation"
	val numberOfBuckets = 10

	def generatePredictionAndLabels(
		training: RDD[((UUID, UUID), Double)],
		test: RDD[((UUID, UUID), Double)]
	): RDD[(Double, Double)] = {
		training
		    .fullOuterJoin(test)
		    .mapValues {
				// True Positives
				case (Some(prediction), Some(1)) => (prediction, 1)
				// False Positives
				case (Some(prediction), None) => (prediction, 0:Double)
				// False negatives
				case (None, Some(1)) => (0:Double, 1)
			}
		    .values
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName(this.appName)
		val sc = new SparkContext(conf)

		val training = sc.cassandraTable[((UUID, UUID), Double)]("datalake", "goldstandard")
		val test = sc
			.cassandraTable[(UUID, UUID)]("datalake", "duplicatecandidates")
		    .map(pair => (pair, 1:Double))

		val predictionAndLabels = generatePredictionAndLabels(training, test)
		val metrics = new BinaryClassificationMetrics(predictionAndLabels, this.numberOfBuckets)

		val precision = metrics.precisionByThreshold
		val recall = metrics.recallByThreshold
		val f1Score = metrics.fMeasureByThreshold
	}
}
