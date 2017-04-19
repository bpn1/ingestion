package de.hpi.ingestion.deduplication

import java.util.UUID

import de.hpi.ingestion.datalake.models.Subject
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.deduplication.models.DuplicateCandidates

class SimilarityMeasureEvaluation(
	appName: String,
	numberOfBuckets: Int = 10
) {
	def getLabel(subject: UUID, duplicate: UUID)(implicit rdd: RDD[DuplicateCandidates]): Double = {
		implicit def doubleToBool(bool: Boolean): Double = if bool 0 else 1
		rdd
			.filter(_.subject_id == subject)
			.filter { case DuplicateCandidates(id, candidates) =>
				candidates.map(_._1.id).contains(duplicate)
			}
		    .collect
		    .isEmpty:Double
	}

	def generatePredictionAndLabels(
		training: RDD[DuplicateCandidates],
		test: RDD[DuplicateCandidates]
	): RDD[(Double, Double)] = {
		implicit val rdd = training
		test
			.map { case DuplicateCandidates(id, candidates) =>
				((id, candidates.head._1.id), candidates.head._3)
			}
		    .map { case ((subject, duplicate), label) =>
				(getLabel(subject, duplicate), label)
			}
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName(this.appName)
		val sc = new SparkContext(conf)

		val training = sc.cassandraTable[DuplicateCandidates]("datalake", "goldstandard")
		val test = sc.cassandraTable[DuplicateCandidates]("datalake", "duplicatecandidates")

		val predictionAndLabels = generatePredictionAndLabels(training, test)
		val metrics = new BinaryClassificationMetrics(predictionAndLabels, this.numberOfBuckets)

		val precision = metrics.precisionByThreshold
		val recall = metrics.recallByThreshold
		val f1Score = metrics.fMeasureByThreshold
	}
}
