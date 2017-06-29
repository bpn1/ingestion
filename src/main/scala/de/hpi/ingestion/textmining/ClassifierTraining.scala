package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models._
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import de.hpi.ingestion.deduplication.models.{PrecisionRecallDataTuple, SimilarityMeasureStats}
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Trains a Naive Bayes model with Feature Entries and evaluates it using precision, recall and f-score.
  */
object ClassifierTraining extends SparkJob {
	appName = "Classifier Training"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	var trainingFunction = crossValidateAndEvaluate _

	/**
	  * Loads Feature entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val entries = sc.cassandraTable[FeatureEntry](settings("keyspace"), settings("secondOrderFeatureTable"))
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
		val List(stats, model) = output
		stats
			.asInstanceOf[RDD[SimilarityMeasureStats]]
			.saveToCassandra(settings("keyspace"), settings("simMeasureStatsTable"))
		model
			.asInstanceOf[RDD[Option[RandomForestModel]]]
			.first
			.foreach(_.save(sc, s"wikipedia_randomforest_model_${System.currentTimeMillis()}"))
	}
	// $COVERAGE-ON$

	/**
	  * Uses the input data to train a Naive Bayes model.
	  *
	  * @param trainingData RDD of labeled points
	  * @return the trained model
	  */
	def trainNaiveBayes(trainingData: RDD[LabeledPoint]): NaiveBayesModel = {
		NaiveBayes.train(trainingData, 1.0)
	}

	/**
	  * Uses the provided data to train a Random Forest model and returns the model.
	  *
	  * @param trainingData RDD of labeled points used to train the model
	  * @param numTrees     number of trees used
	  * @param maxDepth     maximum depth used
	  * @param maxBins      maximum number of bins used
	  * @return the trained model
	  */
	def trainRandomForest(
		trainingData: RDD[LabeledPoint],
		numTrees: Int,
		maxDepth: Int,
		maxBins: Int
	): RandomForestModel = {
		RandomForest.trainClassifier(
			trainingData,
			numClasses = 2,
			categoricalFeaturesInfo = Map[Int, Int](),
			numTrees = numTrees,
			featureSubsetStrategy = "auto",
			impurity = "gini",
			maxDepth = maxDepth,
			maxBins = maxDepth)
	}

	/**
	  * Calculates Precision, Recall and F1-Score of the given prediction results.
	  *
	  * @param predictionAndLabels RDD containing Tuples of the prediction and the given label in the format
	  *                            (prediction, label)
	  * @return List of statistic data as three-tuples in the format (statistic, threshold, value)
	  */
	def calculateStatistics(
		predictionAndLabels: RDD[(Double, Double)],
		beta: Double = 1.0
	): PrecisionRecallDataTuple = {
		val pnCount = predictionAndLabels
			.map { case (prediction, label) =>
				val trueOrFalse = if(prediction == label) "t" else "f"
				val positiveOrNegative = if(prediction == 1.0) "p" else "n"
				(s"$trueOrFalse$positiveOrNegative", 1)
			}.reduceByKey(_ + _)
			.collect
			.toMap
		val tp = pnCount.getOrElse("tp", 0)
		val fp = pnCount.getOrElse("fp", 0)
		val fn = pnCount.getOrElse("fn", 0)
		val precision = Option(tp.toDouble / (tp + fp).toDouble)
		    .filterNot(_.isNaN)
		    .getOrElse(0.0)
		val recall = Option(tp.toDouble / (tp + fn).toDouble)
			.filterNot(_.isNaN)
			.getOrElse(0.0)
		val fMeasure = Option(((1.0 + beta * beta) * precision * recall) / ((beta * beta * precision) + recall))
			.filterNot(_.isNaN)
			.getOrElse(0.0)
		PrecisionRecallDataTuple(1.0, precision, recall, fMeasure)
	}

	/**
	  * Cross validates a Random Forest model with the input data and the given number of folds.
	  *
	  * @param data     input data used to train the classifier
	  * @param numFolds number of folds used for the cross validation
	  * @param numTrees number of trees used
	  * @param maxDepth maximum depth
	  * @param maxBins  number of bins used
	  * @return statistical data for each fold
	  */
	def crossValidate(
		data: RDD[LabeledPoint],
		numFolds: Int,
		numTrees: Int,
		maxDepth: Int,
		maxBins: Int
	): (List[PrecisionRecallDataTuple], List[RandomForestModel]) = {
		val weights = (0 until numFolds).map(t => 1.0 / numFolds).toArray
		val folds = data.randomSplit(weights)
		val segments = folds.indices.map { index =>
			val test = folds(index)
			val training = folds.slice(0, index) ++ folds.slice(index + 1, folds.length)
			(test, training)
		}
		segments
			.map { case (test, trainingList) =>
				val training = trainingList.reduce(_.union(_))
				val model = trainRandomForest(training, numTrees, maxDepth, maxBins)
				val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
					val prediction = model.predict(features)
					(prediction, label)
				}
				(calculateStatistics(predictionAndLabels), model)
			}.toList
			.unzip
	}

	/**
	  * Averages a List of Precision Recall Data Tuples. Uses the Threshold of the first element or 1.0 as default.
	  *
	  * @param stats List of input statistics to average
	  * @return Precision Recall Data Tuples with the mean precision, recall & fscore
	  */
	def averageStatistics(stats: List[PrecisionRecallDataTuple]): PrecisionRecallDataTuple = {
		val n = stats.length
		val threshold = stats.headOption.map(_.threshold).getOrElse(1.0)
		stats.map { case PrecisionRecallDataTuple(statThreshold, precision, recall, fscore) =>
			(precision, recall, fscore)
		}.unzip3 match {
			case (precision, recall, fscore) =>
				PrecisionRecallDataTuple(threshold, precision.sum / n, recall.sum / n, fscore.sum / n)
		}
	}

	/**
	  * Trains and cross validates a Random Forest Model.
	  *
	  * @param data     data used to train and test the model
	  * @param numFolds number of folds used for the cross validation
	  * @param numTrees number of trees used
	  * @param maxDepth maximum depth
	  * @param maxBins  number of bins used
	  * @return statistics of the cross validation and a Random Forest Model
	  */
	def crossValidateAndEvaluate(
		data: RDD[LabeledPoint],
		numFolds: Int = 3,
		numTrees: Int = 5,
		maxDepth: Int = 4,
		maxBins: Int = 32
	): (SimilarityMeasureStats, Option[RandomForestModel]) = {
		val (cvResult, models) = crossValidate(data, numFolds, numTrees, maxDepth, maxBins)
		val statistic = averageStatistics(cvResult)
		val model = models.headOption
		val results = SimilarityMeasureStats(data = List(statistic), comment = Option("Random Forest Cross Validation"))
		(results, model)
	}

	/**
	  * Partitions the entries to train and test a Classification model.
	  * Example code: https://spark.apache.org/docs/latest/mllib-naive-bayes.html
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val entries = input.fromAnyRDD[FeatureEntry]().head
		val data = entries.map(_.labeledPoint()).cache
		val (stats, model) = trainingFunction(data, 5, 5, 4, 32)
		val statsList = List(sc.parallelize(List(stats))).toAnyRDD()
		val modelList = List(sc.parallelize(List(model))).toAnyRDD()
		statsList ++ modelList

		//		Code outcommented due to missing tests
		//		val Array(train, test) = data.randomSplit(Array(0.7, 0.3))
		//		val model = trainRandomForest(train, 100, 4, 32)
		//		val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
		//			val prediction = model.predict(features)
		//			(prediction, label)
		//		}
		//		val stats = calculateStatistics(predictionAndLabels)
		//		val results = SimilarityMeasureStats(data = List(stats),
		//			comment = Option("Random Forest Split Validation"))
		//		val modelOption = sc.parallelize(List(Option(model)))
		//		List(sc.parallelize(List(results))).toAnyRDD() ++ List(modelOption).toAnyRDD()
	}
}
