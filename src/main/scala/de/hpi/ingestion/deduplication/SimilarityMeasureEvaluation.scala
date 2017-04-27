package de.hpi.ingestion.deduplication

import java.util.UUID

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

import org.apache.spark.rdd.RDD
import play.api.libs.json._
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.deduplication.models.{DuplicateCandidates, SimilarityMeasureStats, PrecisionRecallDataTuple}

object SimilarityMeasureEvaluation {
	val appName = "SimilarityMeasureEvaluation"
	val numberOfBuckets = 10
	val inputKeyspace = "evaluation"
	val inputTest = "goldstandard"
	val inputTraining = "dbpedia_wikidata_deduplication"
	val statsKeyspace = "datalake"
	val output = "sim_measure_stats"

	/**
	  * Generate (prediction, label) tuples for evaluation
	  * @param training Precictions from the training data
	  * @param test Labels from the testdata
	  * @return RDD containing (prediction, label) tuples
	  */
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
				case (Some(prediction), None) => (prediction, 0.0)
				// False negatives
				case (None, Some(label)) => (0.0, label)
				case _ => (-1.0, -1.0)
			}
			.values
	}

	def generateStats(
		test: RDD[((UUID, UUID), Double)],
		training: RDD[((UUID,UUID),Double)],
		sc: SparkContext
	) : SimilarityMeasureStats = {
		val predictionAndLabels = generatePredictionAndLabels(training, test)
		val metrics = new BinaryClassificationMetrics(predictionAndLabels, this.numberOfBuckets)

		val precision = metrics.precisionByThreshold
		val recall = metrics.recallByThreshold
		val f1Score = metrics.fMeasureByThreshold

		val data = precision
		.join(recall)
		.join(f1Score)
		.map { case (threshold, ((precision, recall), f1Score)) =>
			PrecisionRecallDataTuple(
				argument = threshold,
				precision = precision,
				recall = recall,
				fscore = f1Score
			)
		}
		.sortBy(_.argument)
		.collect
		.toList

		implicit val statsFormat = Json.format[PrecisionRecallDataTuple]

		val stats = Json.obj("stats" -> data)

		val record = SimilarityMeasureStats(data = stats, comment = Option("Naive Deduplication"))
		println(record)
		record
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName(this.appName)
		val sc = new SparkContext(conf)

		val training = sc
			.cassandraTable[DuplicateCandidates](this.inputKeyspace, this.inputTraining)
			.flatMap { case DuplicateCandidates(subject_id, candidates) =>
				candidates.map(candidate => ((subject_id, candidate._1.id), candidate._3))
			}.distinct
		val test = sc
			.cassandraTable[(UUID, UUID)](this.inputKeyspace, this.inputTest)
			.map(pair => (pair, 1.0))

		val stats = generateStats(test, training, sc)

		sc
			.parallelize(Seq(stats))
			.saveToCassandra(this.statsKeyspace, this.output)
	}
}
