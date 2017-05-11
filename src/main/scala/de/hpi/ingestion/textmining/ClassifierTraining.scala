package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models._
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import de.hpi.ingestion.deduplication.models.{PrecisionRecallDataTuple, SimilarityMeasureStats}
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.P
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.ml.linalg.Vector

/**
  * Trains a Naive Bayes model with Feature Entries and evaluates it using precision, recall and f-score.
  */
object ClassifierTraining extends SparkJob {
	appName = "Classifier Training"
	val keyspace = "wikidumps"
	val tablename = "featureentries"
	val statisticsTable = "sim_measure_stats"

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
			.fromAnyRDD[SimilarityMeasureStats]()
			.head
			.saveToCassandra(keyspace, statisticsTable)
//			.first
//			.save(sc, s"wikipedia_randomforest_model_${System.currentTimeMillis()}")
	}
	// $COVERAGE-ON$

	/**
	  * Uses 60% of the data to train a Naive Bayes model and returns it with 40% of the data which is used for testing.
	  * @param data RDD of labeled points
	  * @return tuple of the trained model and test data in the format (model, testdata)
	  */
	def trainNaiveBayes(data: RDD[LabeledPoint]): (NaiveBayesModel, RDD[LabeledPoint]) = {
		val Array(training, test) = data.randomSplit(Array(0.6, 0.4))
		val model = NaiveBayes.train(training, 1.0)
		(model, test)
	}

	/**
	  * Uses the provided data to train a Random Forest model and returns the model.
	  * @param trainingData RDD of labeled points used to train the model
	  * @param numTrees number of trees used
	  * @return the trained model
	  */
	def trainRandomForest(trainingData: RDD[LabeledPoint], numTrees: Int = 5): RandomForestModel = {
		RandomForest.trainClassifier(
			trainingData,
			numClasses = 2,
			categoricalFeaturesInfo = Map[Int, Int](),
			numTrees = numTrees,
			featureSubsetStrategy = "auto",
			impurity = "gini",
			maxDepth = 4,
			maxBins = 32)
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
		val metrics = new BinaryClassificationMetrics(predictionAndLabels)
		val precision = metrics.precisionByThreshold().filter(_._1 == 1.0).values.first
		val recall = metrics.recallByThreshold().filter(_._1 == 1.0).values.first
		val f1score = metrics.fMeasureByThreshold(beta).filter(_._1 == 1.0).values.first
		PrecisionRecallDataTuple(1.0, precision, recall, f1score)
	}

	/**
	  * Formats List of statistic three-tuples into a printable String.
	  *
	  * @param stats List of statistic three-tuples in the format (statistic, threshold, value)
	  * @return formatted String of the statistics
	  */
	def formatStatistics(stats: PrecisionRecallDataTuple): String = {
		val thresh = s"threshold: ${stats.threshold}"
		val precision = s"precision: ${stats.precision}"
		val recall = s"recall: ${stats.recall}"
		val fscore = s"fscore: ${stats.fscore}"
		s"$thresh\t$precision\t$recall\t$fscore"
	}

	def crossValidate(data: RDD[LabeledPoint], numFolds: Int = 3): List[PrecisionRecallDataTuple] = {
		val weights = (0 until numFolds).map(t => 1.0 / numFolds).toArray
		val folds = data.randomSplit(weights)
		val segments = folds.indices.map { index =>
			val test = folds(index)
			val training = folds.slice(0, index) ++ folds.slice(index + 1, folds.length)
			(test, training)
		}
		segments.map { case (test, trainingList) =>
			val model = trainRandomForest(trainingList.reduce(_.union(_)))
			val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
				val prediction = model.predict(features)
				(prediction, label)
			}
			calculateStatistics(predictionAndLabels)
		}.toList
	}

	def averageStatistics(stats: List[PrecisionRecallDataTuple]): PrecisionRecallDataTuple = {
		val n = stats.length
		val threshold = stats.headOption.map(_.threshold).getOrElse(1.0)
		stats.map { case PrecisionRecallDataTuple(threshold, precision, recall, fscore) =>
			(precision, recall, fscore)
		}.unzip3 match {
			case (precision, recall, fscore) =>
				PrecisionRecallDataTuple(threshold, precision.sum / n, recall.sum / n, fscore.sum / n)
		}
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
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val entries = input.fromAnyRDD[FeatureEntry]().head
		val data = entries.map(_.labeledPoint()).cache
		val cvResult = crossValidate(data)
		val statistic = averageStatistics(cvResult)
		val results = SimilarityMeasureStats(data = List(statistic), comment = Option("Random Forest Cross Validation"))
		List(sc.parallelize(List(results))).toAnyRDD()
	}
//		val Array(training, test) = data.randomSplit(Array(0.99, 0.01))
//
//		val sqlContext = new SQLContext(sc)
//		import sqlContext.implicits._
//		val dfTraining = training.map(lp => (lp.features.asML, lp.label)).toDF("features", "label")
//		val rfClassifier = new RandomForestClassifier()
//			.setNumTrees(5)
//			.setMaxDepth(4)
//			.setMaxBins(32)
//			.setImpurity("gini")
//			.setFeatureSubsetStrategy("auto")
//			.setFeaturesCol("features")
//			.setLabelCol("label")
//		val pipeline = new Pipeline().setStages(Array(rfClassifier))
//		val paramGrid = new ParamGridBuilder().build()
//		val evaluator = new BinaryClassificationEvaluator()
//			.setLabelCol("label")
//			.setRawPredictionCol("prediction")
//			.setMetricName("areaUnderPR")
//		val cv = new CrossValidator()
//			.setEstimator(pipeline)
//			.setEvaluator(evaluator)
//			.setEstimatorParamMaps(paramGrid)
//			.setNumFolds(3)
//
//		val cvModel = cv.fit(dfTraining)
//		cvModel.transform(test.map(lp => (lp.features.asML, lp.label)).toDF("features", "label"))
//			.select("probability", "prediction", "label")
//			.collect
//			.foreach { case Row(prob: Vector, prediction: Double, label: Double) =>
//				println(s"prob: $prob\tprediction: $prediction\tlabel: $label")
//			}

//		val (model, test) = trainRandomForest(data)
//		val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
//			val prediction = model.predict(features)
//			(prediction, label)
//		}
//		val statistics = calculateStatistics(predictionAndLabels, 0.5)
//		println(s"num trees: ${formatStatistics(statistics)}")
//		List(sc.parallelize(List(model))).toAnyRDD()
//		Nil
//	}
}
