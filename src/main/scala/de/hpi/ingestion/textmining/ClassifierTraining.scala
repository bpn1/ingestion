/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.deduplication.models.{PrecisionRecallDataTuple, SimilarityMeasureStats}
import de.hpi.ingestion.framework.SparkJob
import com.datastax.spark.connector._
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext

/**
  * Trains a `RandomForestModel` with `FeatureEntries` and evaluates it in terms of precision, recall and f-score.
  */
class ClassifierTraining extends SparkJob {
	import ClassifierTraining._
	appName = "Classifier Training"
	configFile = "textmining.xml"
	sparkOptions("spark.yarn.executor.memoryOverhead") = "8192"

	var featureEntries: RDD[FeatureEntry] = _
	var similarityMeasureStats: RDD[SimilarityMeasureStats] = _
	var model: Option[PipelineModel] = None

	// $COVERAGE-OFF$
	/**
	  * Loads Feature entries from the Cassandra.
	  * @param sc   Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		featureEntries = sc.cassandraTable[FeatureEntry](settings("keyspace"), settings("companyFeatureEntries"))
	}

	/**
	  * Saves Random Forest Model to the HDFS.
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		similarityMeasureStats.saveToCassandra(settings("keyspace"), settings("simMeasureStatsTable"))
		model.foreach(_.save(s"wikipedia_tuned_df_randomforest_${System.currentTimeMillis()}"))
	}
	// $COVERAGE-ON$

	/**
	  * Partitions the entries to train and test a Classification model.
	  * @param sc    Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		val session = SparkSession.builder().getOrCreate()
		val (simStats, pipelineModel) = crossValidateAndEvaluate(
			featureEntries,
			3,
			randomForestDFModel(),
			session)
		model = pipelineModel
		similarityMeasureStats = sc.parallelize(List(simStats))
	}
}

object ClassifierTraining {
	/**
	  * Calculates Precision, Recall and F-Score of the given prediction results.
	  * @param pnCount Map containing the counts of tp, fp, tn and fn.
	  * @param beta beta value used for the F-Score.
	  * @return Precision, Recall and F-Score
	  */
	def calculatePrecisionRecall(pnCount: Map[String, Int], beta: Double = 1.0): PrecisionRecallDataTuple = {
		val tp = pnCount.getOrElse("tp", 0)
		val fp = pnCount.getOrElse("fp", 0)
		val fn = pnCount.getOrElse("fn", 0)
		val precision = tp.toDouble / (tp + fp).toDouble
		val recall = tp.toDouble / (tp + fn).toDouble
		val fMeasure = ((1.0 + beta * beta) * precision * recall) / ((beta * beta * precision) + recall)
		PrecisionRecallDataTuple(1.0, precision, recall, fMeasure)
	}

	/**
	  * Calculates Precision, Recall and F-Score of the given prediction results.
	  * @param predictionAndLabels RDD containing the prediction results in the format of (prediction, label) tuples.
	  * @param beta beta value used for the F-Score.
	  * @return Precision, Recall and F-Score
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
		calculatePrecisionRecall(pnCount, beta)
	}

	/**
	  * Calculates Precision, Recall and F-Score of the given prediction results. The Precision and Recall are adjusted
	  * for collisions of predicted entities (multiple entities at the same offset in the same articles that were
	  * predicted to be true).
	  * @param predictionAndLabels RDD containing the prediction results in the format of (prediction, label) tuples.
	  * @param beta beta value used for the F-Score.
	  * @return Precision, Recall and F-Score
	  */
	def calculateAdvancedStatistics(
		predictionAndLabels: RDD[((String, Int), List[(Double, Double)])],
		beta: Double = 1.0
	): PrecisionRecallDataTuple = {
		val pnCount = predictionAndLabels
			.reduceByKey(_ ++ _)
			.values
			.flatMap { predictionList =>
				val (positives, negatives) = predictionList.partition(_._1 == 1.0)
				val multipleMatches = positives.length > 1
				val negativeCount = negatives.map { case (prediction, label) =>
					(trueOrFalsePrediction(prediction, label, "n"), 1)
				}
				val positiveCount = positives.map {
					case (prediction, label) if multipleMatches => ("fn", 1)
					case (prediction, label) if !multipleMatches => (trueOrFalsePrediction(prediction, label, "p"), 1)
				}
				positiveCount ++ negativeCount
			}.reduceByKey(_ + _)
			.collect
			.toMap
		calculatePrecisionRecall(pnCount, beta)
	}

	/**
	  * Parses a prediction and its label to a key used for the precision and recall calculation.
	  * @param prediction prediction of the entry
	  * @param label label of the entry
	  * @param statisticLabel label used for the entry (either p for positive or n for negative)
	  * @return key used for this entry
	  */
	def trueOrFalsePrediction(prediction: Double, label: Double, statisticLabel: String): String = {
		val trueOrFalse = if(prediction == label) "t" else "f"
		s"$trueOrFalse$statisticLabel"
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
	  * Uses the input data to train a Naive Bayes model.
	  *
	  * @param trainingData RDD of labeled points
	  * @return the trained model
	  */
	def trainNaiveBayes(trainingData: RDD[LabeledPoint]): NaiveBayesModel = {
		NaiveBayes.train(trainingData, 1.0)
	}

	/**
	  * Sets the parameters for a Dataframe based Random Forest Classifier.
	  * @param maxDepth maximum depth of the trees
	  * @param maxBins maximum bins used
	  * @param numTrees number of trees used
	  * @param thresholds Array of the thresholds (weights) used
	  * @return Random Forest Classifier with the set parameters
	  */
	def randomForestDFModel(
		maxDepth: Int = 6,
		maxBins: Int = 40,
		numTrees: Int = 20,
		thresholds: Array[Double] = Array(1.0, 1.0)
	): RandomForestClassifier = {
		new RandomForestClassifier()
			.setLabelCol("indexedLabel")
			.setFeaturesCol("indexedFeatures")
			.setNumTrees(numTrees)
			.setImpurity("gini")
			.setMaxBins(maxBins)
			.setMaxDepth(maxDepth)
			.setThresholds(thresholds)
			.setFeatureSubsetStrategy("auto")
	}

	/**
	  * Sets the parameters for a Logistic Regression classifier.
	  * @return Logistic Regression classifier with set parameters
	  */
	def weightedLogisticRegressionDFModel(): LogisticRegression = {
		new LogisticRegression()
			.setWeightCol("classWeightCol")
			.setLabelCol("indexedLabel")
			.setFeaturesCol("features")
	}

	/**
	  * Trains a Logistic Regression classifier with the given data and parameters.
	  * @param training training data
	  * @param classifier Logistic Regression classifier with set parameters
	  * @return trained Pipeline Model
	  */
	def trainWeightedLogisticRegressionDF(
		training: Dataset[Row],
		classifier: LogisticRegression
	): PipelineModel = {
		val labelIndexer = new StringIndexer()
			.setInputCol("label")
			.setOutputCol("indexedLabel")
			.fit(training)

		val labelConverter = new IndexToString()
			.setInputCol("prediction")
			.setOutputCol("predictedLabel")
			.setLabels(labelIndexer.labels)

		val pipeline = new Pipeline()
			.setStages(Array(labelIndexer, classifier, labelConverter))
		pipeline.fit(training)
	}

	/**
	  * Trains a Dataframe based Random Forest Model using the provided training data.
	  * @param training  Dataset containing the Labeled Points as Row
	  * @param classifier Random Forest Classifier with set parameters
	  * @return Pipeline Model containing the Random Forest Classifier
	  */
	def trainRandomForestDF(
		training: Dataset[Row],
		classifier: RandomForestClassifier
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

		val labelConverter = new IndexToString()
			.setInputCol("prediction")
			.setOutputCol("predictedLabel")
			.setLabels(labelIndexer.labels)

		val pipeline = new Pipeline()
			.setStages(Array(labelIndexer, featureIndexer, classifier, labelConverter))
		pipeline.fit(training)
	}

	/**
	  * Transforms a RDD of Feature Entries to a Dataset of Dataframe Labeled Points.
	  * @param data RDD of Feature Entries
	  * @param session Spark Session used to access the Dataframe API
	  * @return Dataset of Labeled Points as Row
	  */
	def labeledPointDF(data: RDD[FeatureEntry], session: SparkSession): Dataset[Row] = {
		import session.implicits._
		data
			.map { entry =>
				val labeledPoint = entry.labeledPointDF()
				(labeledPoint.features, labeledPoint.label, entry.article, entry.offset)
			}.toDF("features", "label", "article", "offset")
	}

	/**
	  * Cross validates a Random Forest model with the input data and the given number of folds.
	  *
	  * @param data     input data used to train the classifier
	  * @param numFolds number of folds used for the cross validation
	  * @param classifier Random Forest Classifier containing the used parameters
	  * @param session  current Spark Session
	  * @return statistical data for each fold
	  */
	def crossValidate(
		data: RDD[FeatureEntry],
		numFolds: Int,
		classifier: RandomForestClassifier,
		session: SparkSession
	): (List[PrecisionRecallDataTuple], List[PipelineModel]) = {
		val weights = Array.fill(numFolds)(1.0)
		val folds = data
			.map(entry => ((entry.article, entry.offset), List(entry)))
			.reduceByKey(_ ++ _)
			.values
			.randomSplit(weights)
			.map(_.flatMap(identity))
		val segments = folds.zipWithIndex.map { case (test, index) =>
			val training = folds.slice(0, index) ++ folds.slice(index + 1, folds.length)
			(test, training)
		}
		segments
			.map { case (test, trainingList) =>
				val training = trainingList.reduce(_.union(_))
				val filteredTraining = training
					.filter(entry => !(entry.entity_score.rank > 9 || entry.cosine_sim.rank > 9))
				val trainingDF = labeledPointDF(filteredTraining, session)
				val testDF = labeledPointDF(test, session)
				val model = trainRandomForestDF(trainingDF, classifier)
				val predictions = model.transform(testDF)
				val predictionAndLabels = predictions.rdd.map { row =>
					val prediction = row.getDouble(row.fieldIndex("prediction"))
					val label = row.getDouble(row.fieldIndex("indexedLabel"))
					val article = row.getString(row.fieldIndex("article"))
					val offset = row.getInt(row.fieldIndex("offset"))
					((article, offset), List((prediction, label)))
				}
				(calculateAdvancedStatistics(predictionAndLabels), model)
			}.toList
			.unzip
	}

	/**
	  * Trains and cross validates a Random Forest Model.
	  *
	  * @param data data used to train and test the model
	  * @param numFolds number of folds used for the cross validation
	  * @param classifier Random Forest Classifier containing the used parameters
	  * @param session current Spark Session
	  * @return statistics of the cross validation and a Random Forest Model
	  */
	def crossValidateAndEvaluate(
		data: RDD[FeatureEntry],
		numFolds: Int = 3,
		classifier: RandomForestClassifier,
		session: SparkSession
	): (SimilarityMeasureStats, Option[PipelineModel]) = {
		val (cvResult, models) = crossValidate(data, numFolds, classifier, session)
		val statistic = averageStatistics(cvResult)
		val model = models.headOption
		val results = SimilarityMeasureStats(
			data = List(statistic),
			comment = Option(s"RF CV $numFolds folds"),
			yaxis = Option("P/R/F1"))
		(results, model)
	}
}
