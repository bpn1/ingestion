package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models._
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import de.hpi.ingestion.deduplication.models.{PrecisionRecallDataTuple, SimilarityMeasureStats}
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.ml.feature.{LabeledPoint => DFLabeledPoint}
import org.apache.spark.ml.linalg.{DenseVector => DFDenseVector}

/**
  * Trains a Random Forest model with Feature Entries and evaluates it using precision, recall and f-score.
  */
object ClassifierTraining extends SparkJob {
	appName = "Classifier Training"
	configFile = "textmining.xml"
	sparkOptions("spark.yarn.executor.memoryOverhead") = "8192"

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
		val entries = sc.cassandraTable[FeatureEntry](settings("keyspace"), "featureentries_testing")
		//settings("secondOrderFeatureTable"))
		List(entries).toAnyRDD()
	}

	/**
	  * Saves Random Forest Model to the HDFS.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
//		val List(stats, model) = output
		output
		    .head
			.asInstanceOf[RDD[SimilarityMeasureStats]]
			.saveToCassandra(settings("keyspace"), settings("simMeasureStatsTable"))
//		model
//			.asInstanceOf[RDD[Option[RandomForestModel]]]
//			.first
//			.foreach(_.save(sc, s"wikipedia_randomforest_testing_${System.currentTimeMillis()}"))
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
		println(s"\npn count:\n${pnCount.mkString("\n")}")
		val tp = pnCount.getOrElse("tp", 0)
		val fp = pnCount.getOrElse("fp", 0)
		val fn = pnCount.getOrElse("fn", 0)
		val precision = tp.toDouble / (tp + fp).toDouble
		val recall = tp.toDouble / (tp + fn).toDouble
		val fMeasure = ((1.0 + beta * beta) * precision * recall) / ((beta * beta * precision) + recall)
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

	def crossValidateDF(
//		data: Dataset[Row],
		data: RDD[FeatureEntry],
		threshold: Double,
		numFolds: Int = 3,
		session: SparkSession
	): (List[PrecisionRecallDataTuple], List[PipelineModel]) = {
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
					.filter(entry => !(entry.entity_score.rank > 9 || entry.cosine_sim.rank > 9))
				val trainingDF = labeledPointDF(training, session)
				val model = trainRandomForestDF(trainingDF, Array(threshold, 1.0))
				val predictions = model.transform(labeledPointDF(test, session))
				val predictionAndLabels = predictions.rdd.map { row =>
					val prediction = row.getDouble(row.fieldIndex("prediction"))
					val label = row.getDouble(row.fieldIndex("indexedLabel"))
					(prediction, label)
				}
				(calculateStatistics(predictionAndLabels).copy(threshold = threshold), model)
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

	def crossValidateAndEvaluateDF(
//		data: Dataset[Row],
		data: RDD[FeatureEntry],
		numFolds: Int = 3,
		session: SparkSession
	): SimilarityMeasureStats = {
		val statistics = (10 to 20).map { i =>
			val threshold = i.toDouble / 10.0
			val (cvResult, models) = crossValidateDF(data, threshold, numFolds, session)
			averageStatistics(cvResult)
		}.toList
		val comment = "RF CV w/ filtered Triehits and variable thresholds"
		val results = SimilarityMeasureStats(data = statistics, comment = Option(comment))
		results
	}

	def trainRandomForestDF(
		training: Dataset[Row],
		thresholds: Array[Double] = Array(1.0, 1.0),
		maxDepth: Int = 4,
		maxBins: Int = 32,
		maxTrees: Int = 5
	): PipelineModel = {
		val labelIndexer = new StringIndexer()
			.setInputCol("label")
			.setOutputCol("indexedLabel")
			.fit(training)

		val featureIndexer = new VectorIndexer()
			.setInputCol("features")
			.setOutputCol("indexedFeatures")
			.setMaxCategories(4)
			.fit(training)

		val rf = new RandomForestClassifier()
			.setLabelCol("indexedLabel")
			.setFeaturesCol("indexedFeatures")
			.setNumTrees(maxTrees)
			.setImpurity("gini")
			.setMaxBins(maxBins)
			.setMaxDepth(maxDepth)
			.setThresholds(thresholds)
			.setFeatureSubsetStrategy("auto")

		val labelConverter = new IndexToString()
			.setInputCol("prediction")
			.setOutputCol("predictedLabel")
			.setLabels(labelIndexer.labels)

		val pipeline = new Pipeline()
			.setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))
		pipeline.fit(training)
	}

	def labeledPointDF(data: RDD[FeatureEntry], session: SparkSession): Dataset[Row] = {
		import session.implicits._
		data
			.map(_.labeledPoint())
			.map { case LabeledPoint(label, feature) =>
				val vector = new DFDenseVector(feature.toArray)
				DFLabeledPoint(label, vector)
			}.toDF()
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
		val session = SparkSession.builder().getOrCreate()
		val entries = input.fromAnyRDD[FeatureEntry]().head
//		val results = crossValidateAndEvaluateDF(entries, 5, session)
		val Array(training, test) = entries
			.randomSplit(Array(0.7, 0.3))
		val filteredTraining = training.filter(entry => !(entry.entity_score.rank > 9 || entry.cosine_sim.rank > 9))
		val trainingDF = labeledPointDF(filteredTraining, session).cache()
		val testDF = labeledPointDF(test, session).cache()

		val stepSize = 5
		val statistics = (1 to 40).map { i =>
			val trees = i * stepSize
			val model = trainRandomForestDF(trainingDF, maxDepth = 6, maxBins = 40, maxTrees = trees)
			val predictions = model.transform(testDF)
			val predictionAndLabels = predictions.rdd.map { row =>
				val prediction = row.getDouble(row.fieldIndex("prediction"))
				val label = row.getDouble(row.fieldIndex("indexedLabel"))
				(prediction, label)
			}
			calculateStatistics(predictionAndLabels).copy(threshold = trees)
		}.toList
		val comment = "RF w/ filtered Triehits and variable trees"
		val results = SimilarityMeasureStats(data = statistics, comment = Option(comment), xaxis = Option("max trees"))
		List(sc.parallelize(List(results))).toAnyRDD()// ++ List(modelOption).toAnyRDD()
	}
}
