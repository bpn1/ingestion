package de.hpi.ingestion.deduplication

import java.util.UUID
import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.deduplication.models.{DuplicateCandidates, PrecisionRecallDataTuple, SimilarityMeasureStats}
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import scala.xml.Node

object SimilarityMeasureEvaluation extends SparkJob {
	appName = "SimilarityMeasureEvaluation"
	configFile = "similarity_measure_evaluation.xml"

	// $COVERAGE-OFF$
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val training = sc.cassandraTable[DuplicateCandidates](
			settings("keyspaceTrainingTable"),
			settings("trainingTable"))
		val test = sc.cassandraTable[(UUID, UUID)](settings("keyspaceTestTable"), settings("testTable"))
		List(training).toAnyRDD() ++ List(test).toAnyRDD()
	}

	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[SimilarityMeasureStats]()
			.head
			.saveToCassandra(settings("keyspaceSimMeasureStatsTable"), settings("simMeasureStatsTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Calculates precision, recall and f1 score using the training and test data.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val training = input.head.asInstanceOf[RDD[DuplicateCandidates]]
			.flatMap { case DuplicateCandidates(subject_id, candidates) =>
				candidates.map(candidate => ((subject_id, candidate._1), candidate._3))
			}.distinct

		val test = input(1).asInstanceOf[RDD[(UUID, UUID)]].map(pair => (pair, 1.0))
		val predictionAndLabels = generatePredictionAndLabels(training, test)
		val data = generateStats(predictionAndLabels)
		val stats = SimilarityMeasureStats(data = data, comment = Option("Naive Deduplication"))

		List(sc.parallelize(Seq(stats))).toAnyRDD()
	}

	/**
	  * Generate (prediction, label) tuples for evaluation
	  * @param training	Predictions from the training data
	  * @param test		Labels from the test data
	  * @return			RDD containing (prediction, label) tuples
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

	/**
	  * Generates Precision, Recall and FScore
	  * @param predictionAndLabels Data to be generated from
	  * @return List of PrecisionRecallDataTuples
	  */
	def generateStats(predictionAndLabels: RDD[(Double, Double)] ) : List[PrecisionRecallDataTuple] = {
		val buckets = settings("buckets").toInt
		val metrics = new BinaryClassificationMetrics(predictionAndLabels, buckets)

		val precision = metrics.precisionByThreshold
		val recall = metrics.recallByThreshold
		val f1Score = metrics.fMeasureByThreshold

		precision
			.join(recall)
			.join(f1Score)
			.map { case (threshold, ((precision, recall), f1Score)) =>
				PrecisionRecallDataTuple(threshold, precision, recall, f1Score)
			}.sortBy(_.threshold).collect.toList
	}
}
