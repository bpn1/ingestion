package de.hpi.ingestion.deduplication

import java.util.UUID
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.deduplication.models.{DuplicateCandidates, PrecisionRecallDataTuple, SimilarityMeasureStats}
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.Bag

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
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val training = input.head.asInstanceOf[RDD[DuplicateCandidates]]
			.flatMap { case DuplicateCandidates(subject_id, candidates) =>
				candidates.map(candidate => ((subject_id, candidate._1), candidate._3))
			}.distinct
		val test = input(1).asInstanceOf[RDD[(UUID, UUID)]].map(pair => (pair, 1.0))
		implicit val buckets = generateBuckets(this.settings("buckets").toInt)
		val predictionAndLabels = generatePredictionAndLabels(training, test)
		val data = generatePrecisionRecallData(predictionAndLabels)
		val stats = SimilarityMeasureStats(data = data, comment = Option("Naive Deduplication"))

		List(sc.parallelize(Seq(stats))).toAnyRDD()
	}

	/**
	  * Generates a List of Buckets regarding the Config
	  * @return List of Doubles defining the buckets
	  */
	def generateBuckets(size: Int): List[Double] = {
		(0 to size)
			.map(_.toDouble / size.toDouble)
			.toList
	}

	/**
	  * Returns a bucket corresponding to a score
	  * @param score The score
	  * @param buckets The bucket List, to choose the correct bucket from
	  * @return The bucket containing the score
	  */
	def bucket(score: Double)(implicit buckets: List[Double]): Double = {
		val size = buckets.size - 1
		(score * size).toInt / size.toDouble
	}

	/**
	  * Generates PrecisionRecallDataTuple from labeledPoints for each bucket
	  * @param labeledPoints Labeled Points
	  * @param buckets Buckets
	  * @return List of PrecisionRecallDataTuple for each bucket
	  */
	def generatePrecisionRecallData(labeledPoints: RDD[(Double, Double)])
		(implicit buckets: List[Double]): List[PrecisionRecallDataTuple] = {
		val bag = labeledPoints.map { case (prediction, label) =>
			val bucketLabel = bucket(prediction)
			val key = s"$label;$bucketLabel"
			Bag(key -> 1)
		}.reduce(_ ++ _).getCounts()

		buckets.map { bucket =>
			val positiveNegativeCount = bag
				.toList
				.map { case (key, value) =>
					val Array(label, bucketLabel) = key.split(';')
					val positiveOrNegative = if(bucketLabel.toDouble >= bucket) "p" else "n"
					val trueOrFalse = if((label == "1.0") == (positiveOrNegative == "p")) "t" else "f"
					(s"$trueOrFalse$positiveOrNegative", value)
				}.groupBy(_._1)
				.mapValues(_.map(_._2).sum)
				.map(identity)

			val tp = positiveNegativeCount.getOrElse("tp", 0)
			val fp = positiveNegativeCount.getOrElse("fp", 0)
			val fn = positiveNegativeCount.getOrElse("fn", 0)

			val precision = tp.toDouble / (tp + fp).toDouble
			val recall = tp.toDouble / (tp + fn).toDouble
			val fMeasure = (2 * recall * precision) / (recall + precision)
			PrecisionRecallDataTuple(
				bucket,
				if(precision.isNaN) 0.0 else precision,
				recall,
				if(fMeasure.isNaN) 0.0 else fMeasure
			)
		}
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
			.values
			.collect {
				case (prediction, label) if prediction.isDefined || label.isDefined =>
					(prediction.getOrElse(0.0), label.getOrElse(0.0))
			}
	}
}
