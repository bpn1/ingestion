package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models._
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Trains a Naive Bayes model with Feature Entries and evaluates it using precision, recall and f-score.
  */
object ClassifierTraining extends SparkJob {
	appName = "Classifier Training"
	val keyspace = "wikidumps"
	val tablename = "featureentries"

	// $COVERAGE-OFF$
	/**
	  * Loads Feature entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val entries = sc.cassandraTable[FeatureEntry](keyspace, tablename)
		List(entries).toAnyRDD()
	}

	/**
	  * Saves Naive Bayes Model to the HDFS.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[NaiveBayesModel]()
			.head
			.first
			.save(sc, s"wikipedia_naivebayes_model_${System.currentTimeMillis()}")
	}
	// $COVERAGE-ON$

	/**
	  * Calculates Precision, Recall and F1-Score of the given prediction results.
	  *
	  * @param predictionAndLabels RDD containing Tuples of the prediction and the given label in the format
	  *                            (prediction, label)
	  * @return List of statistic data as three-tuples in the format (statistic, threshold, value)
	  */
	def calculateStatistics(
		predictionAndLabels: RDD[(Double, Double)],
		beta: Double
	): List[(String, Double, Double)] = {
		val metrics = new BinaryClassificationMetrics(predictionAndLabels)
		val precision = metrics.precisionByThreshold()
		val recall = metrics.recallByThreshold()
		val f1score = metrics.fMeasureByThreshold(beta)
		val precisionOutput = precision.map { case (t, p) => ("precision", t, p) }
		val recallOutput = recall.map { case (t, r) => ("recall", t, r) }
		val fscoreOutput = f1score.map { case (t, f) => (s"fscore with beta $beta", t, f) }
		precisionOutput
			.union(recallOutput)
			.union(fscoreOutput)
			.collect
			.toList
	}

	/**
	  * Formats List of statistic three-tuples into a printable String.
	  *
	  * @param stats List of statistic three-tuples in the format (statistic, threshold, value)
	  * @return formatted String of the statistics
	  */
	def formatStatistics(stats: List[(String, Double, Double)]): String = {
		stats
			.map(t => s"${t._1}\t${t._2}\t${t._3}")
			.mkString("\n")
	}

	/**
	  * Uses 60% of the entries to train a Naive Bayes model and tests the model with the other 40% of the data.
	  * Example code: https://spark.apache.org/docs/latest/mllib-naive-bayes.html
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val entries = input.fromAnyRDD[FeatureEntry]().head
		val data = entries.map(_.labeledPoint())
		val Array(training, test) = data.randomSplit(Array(0.6, 0.4))

		val model = NaiveBayes.train(training, 1.0)
		val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
			val prediction = model.predict(features)
			(prediction, label)
		}
		val statistics = calculateStatistics(predictionAndLabels, 0.5)

		List(sc.parallelize(Seq(model))).toAnyRDD()
	}
}
