package de.hpi.ingestion.deduplication

import de.hpi.ingestion.framework.{Configurable, SparkJob}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.deduplication.models.FeatureEntry
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.ClassifierTraining.calculateStatistics
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}

/**
  * Job for training the similarity measure classifier
  */
object ClassificationTraining extends SparkJob with Configurable {
	appName = s"Similarity Measure Classifier Training"
	configFile = "classification.xml"

	// $COVERAGE-OFF$
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val entries = sc.cassandraTable[FeatureEntry](settings("keyspaceFeatureTable"), settings("featureTable"))
		List(entries).toAnyRDD()
	}

	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[NaiveBayesModel]()
			.head
			.first
			.save(sc, s"dbpedia_wikidata_naivebayes_model_${System.currentTimeMillis()}")
	}
	// $COVERAGE-ON$

	override def assertConditions(args: Array[String]): Boolean = {
		parseConfig()
		super.assertConditions(args)
	}

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val entries = input.fromAnyRDD[FeatureEntry]().head
		val data = entries.map(_.labeledPoint)
		val Array(training, test) = data.randomSplit(Array(0.6, 0.4), seed = 11L)
		val model = NaiveBayes.train(training, 1.0)
		val predictionAndLabel = test.map(point => (model.predict(point.features), point.label))
		val statistics = calculateStatistics(predictionAndLabel, 0.5)

		List(sc.parallelize(Seq(model))).toAnyRDD()
	}
}
