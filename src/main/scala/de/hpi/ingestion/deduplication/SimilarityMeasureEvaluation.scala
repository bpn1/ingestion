package de.hpi.ingestion.deduplication

import java.util.UUID
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

object SimilarityMeasureEvaluation {
	val appName = "SimilarityMeasureEvaluation"
	val numberOfBuckets = 10
	val keyspace = "evaluation"
	val inputTest = "goldstandard"
	val inputTraining = "dbpedia_wikidata_deduplication"
	val output = "sim_measure_stats"

	def generatePredictionAndLabels(
		training: RDD[((UUID, UUID), Double)],
		test: RDD[((UUID, UUID), Double)]
	): RDD[(Double, Double)] = {
		training
		    .fullOuterJoin(test)
		    .mapValues {
				// True Positives
				case (Some(prediction), Some(label)) => (prediction, label)
				// False Positives
				case (Some(prediction), None) => (prediction, 0:Double)
				// False negatives
				case (None, Some(label)) => (0:Double, label)
				case _ => (-1:Double, -1:Double)
			}
		    .values
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName(this.appName)
		val sc = new SparkContext(conf)

		val training = sc.cassandraTable[((UUID, UUID), Double)](this.keyspace, this.inputTraining)
		val test = sc
			.cassandraTable[(UUID, UUID)](this.keyspace, this.inputTest)
		    .map(pair => (pair, 1:Double))

		val predictionAndLabels = generatePredictionAndLabels(training, test)
		val metrics = new BinaryClassificationMetrics(predictionAndLabels, this.numberOfBuckets)

		val precision = metrics.precisionByThreshold
		val recall = metrics.recallByThreshold
		val f1Score = metrics.fMeasureByThreshold

		precision
			.join(recall)
		    .join(f1Score)
		    .map { case (threshold, ((precision, recall), f1Score)) =>
				(threshold, precision, recall, f1Score)
			}
		    .saveToCassandra(this.keyspace, this.output)
	}
}
