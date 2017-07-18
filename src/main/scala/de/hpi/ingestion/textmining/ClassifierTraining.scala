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
import org.apache.spark.ml.classification.{GBTClassifier, LogisticRegression, RandomForestClassifier, NaiveBayes => DFNaiveBayes}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Dataset, Row, SparkSession}

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
		val entries = sc.cassandraTable[FeatureEntry](settings("keyspace"), "featureentries_testing2")
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
		val List(stats, model) = output
		stats
//		output.head
			.asInstanceOf[RDD[SimilarityMeasureStats]]
			.saveToCassandra(settings("keyspace"), settings("simMeasureStatsTable"))
		model
			.asInstanceOf[RDD[Option[PipelineModel]]]
			.first
			.foreach(_.save(s"wikipedia_tuned_df_randomforest_${System.currentTimeMillis()}"))
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
	  * Sets the parameters for a Dataframe based Random Forest Classifier.
	  * @param thresholds Array of the thresholds (weights) used
	  * @param maxDepth maximum depth of the trees
	  * @param maxBins maximum bins used
	  * @param numTrees number of trees used
	  * @return Random Forest Classifier with the set parameters
	  */
	def randomForestClassifier(
		thresholds: Array[Double] = Array(1.0, 1.0),
		maxDepth: Int = 6,
		maxBins: Int = 40,
		numTrees: Int = 20
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
	  * Trains a Dataframe based Random Forest Model using the provided training data.
	  * @param training  Dataset containing the Labeled Points as Row
	  * @param classifier Random Forest Classifier with set parameters
	  * @return Pipeline Model containing the Random Forest Classifier
	  */
	def trainRandomForestDF(
		training: Dataset[Row],
		classifier: RandomForestClassifier,
		iter: Int = 10
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

		// gradient boost
//		val gbt = new GBTClassifier()
//			.setLabelCol("indexedLabel")
//			.setFeaturesCol("indexedFeatures")
//			.setMaxIter(iter)
//			.setMaxBins(40)
//			.setMaxDepth(6)

		val labelConverter = new IndexToString()
			.setInputCol("prediction")
			.setOutputCol("predictedLabel")
			.setLabels(labelIndexer.labels)

		val pipeline = new Pipeline()
			.setStages(Array(labelIndexer, featureIndexer, classifier, labelConverter))
//			.setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))
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

	def trueOrFalsePrediction(prediction: Double, label: Double, statisticLabel: String): String = {
		val trueOrFalse = if(prediction == label) "t" else "f"
		s"$trueOrFalse$statisticLabel"
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
		calculatePrecisionRecall(pnCount, beta)
	}

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
				val positiveCount = positives.collect {
					case (prediction, label) if multipleMatches && label == 1.0 =>
						("fn", 1)
					case (prediction, label) if !multipleMatches =>
						(trueOrFalsePrediction(prediction, label, "p"), 1)
				}
				positiveCount ++ negativeCount
			}.reduceByKey(_ + _)
			.collect
			.toMap
		calculatePrecisionRecall(pnCount, beta)
	}

	def calculatePrecisionRecall(pnCount: Map[String, Int], beta: Double = 1.0): PrecisionRecallDataTuple = {
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
	  * @param session  current Spark Session
	  * @return statistical data for each fold
	  */
	def crossValidate(
		data: RDD[List[FeatureEntry]],
		numFolds: Int,
		session: SparkSession
	): (List[PrecisionRecallDataTuple], List[PipelineModel]) = {
		val weights = (0 until numFolds).map(t => 1.0 / numFolds).toArray
		val folds = data.randomSplit(weights).map(_.flatMap(identity))
		val segments = folds.indices.map { index =>
			val test = folds(index)
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
				val classifier = randomForestClassifier(
					maxDepth = 6,
					maxBins = 40,
					numTrees = 20,
					thresholds = Array(0.5, 1.0))
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
	  * @param session  current Spark Session
	  * @return statistics of the cross validation and a Random Forest Model
	  */
	def crossValidateAndEvaluate(
		data: RDD[List[FeatureEntry]],
		numFolds: Int = 3,
		session: SparkSession
	): (SimilarityMeasureStats, Option[PipelineModel]) = {
		val (cvResult, models) = crossValidate(data, numFolds, session)
		val statistic = averageStatistics(cvResult)
		val model = models.headOption
		val results = SimilarityMeasureStats(data = List(statistic), comment = Option(s"RF CV $numFolds folds"))
		(results, model)
	}

	/**
	  * Splits the input Feature Entry RDD into 70% training data and 30% test data as Dataframe.
	  * @param entries RDD of Feature Entries
	  * @param session Spark Session used to access Dataframe API
	  * @return Tuple of training and test Datasets
	  */
	def trainingSplit(entries: RDD[FeatureEntry], session: SparkSession): (Dataset[Row], Dataset[Row]) = {
		val Array(training, test) = entries
			.map(entry => ((entry.article, entry.offset), List(entry)))
			.reduceByKey(_ ++ _)
			.randomSplit(Array(0.7, 0.3))
			.map(_.values.flatMap(identity))
		val filteredTraining = training
			.filter(entry => !(entry.entity_score.rank > 9 || entry.cosine_sim.rank > 9))
		val trainingDF = labeledPointDF(filteredTraining, session).cache()
		val testDF = labeledPointDF(test, session).cache()
		(trainingDF, testDF)
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
		val (training, test) = trainingSplit(entries, session)
//		val data = entries
//			.map(entry => ((entry.article, entry.offset), List(entry)))
//			.reduceByKey(_ ++ _)
		val statistics = (1 to 100 by 10).map { i =>
			val thresh = i / 10.0
//			val classifier = randomForestClassifier(maxDepth = 6, maxBins = 40, numTrees = 20)
//			val model = trainRandomForestDF(training, classifier)
			val model = new DFNaiveBayes().setThresholds(Array(i, 1.0)).fit(training)
			val predictions = model.transform(test)
			val predictionAndLabels = predictions.rdd.map { row =>
				val article = row.getString(row.fieldIndex("article"))
				val offset = row.getInt(row.fieldIndex("offset"))
				val prediction = row.getDouble(row.fieldIndex("prediction"))
//				val label = row.getDouble(row.fieldIndex("indexedLabel"))
				val label = row.getDouble(row.fieldIndex("label"))
				((article, offset), List((prediction, label)))
			}
			calculateAdvancedStatistics(predictionAndLabels).copy(threshold = thresh)
		}.toList
//		val classifier = randomForestClassifier(maxDepth = 6, maxBins = 40, numTrees = 20, thresholds = Array(1.0, 1.0))
////		val model = trainRandomForestDF(training, classifier)
//		val model = new LogisticRegression()
//			.setMaxIter(10)
//			.setRegParam(0.3)
//			.setElasticNetParam(0.8)
//			.setThreshold(0.1)
//			.fit(training)
//		val predictions = model.transform(test)
//		val predictionAndLabels = predictions.rdd.map { row =>
//			val article = row.getString(row.fieldIndex("article"))
//			val offset = row.getInt(row.fieldIndex("offset"))
//			val prediction = row.getDouble(row.fieldIndex("prediction"))
////			val label = row.getDouble(row.fieldIndex("indexedLabel"))
//			val label = row.getDouble(row.fieldIndex("label"))
//			((article, offset), List((prediction, label)))
//		}
//		val statistics = List(calculateAdvancedStatistics(predictionAndLabels))
		val comment = "Naive Bayes PR variable threshold"
		val results = SimilarityMeasureStats(data = statistics, comment = Option(comment), xaxis = Option("threshold"), yaxis = Option("P/R/F1"))
//		val (results, model) = crossValidateAndEvaluate(data, 3, session)
//		val modelOption = sc.parallelize(List(Option(model)))
		val modelOption = sc.parallelize(List(None))
		List(sc.parallelize(List(results))).toAnyRDD() ++ List(modelOption).toAnyRDD()
	}
}
