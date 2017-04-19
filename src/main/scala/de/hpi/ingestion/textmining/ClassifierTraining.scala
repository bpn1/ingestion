package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.rdd.RDD

object ClassifierTraining {
	val keyspace = "wikidumps"
	val tablename = "featureentries"

	/**
	  * Uses 60% of the entries to train a Naive Bayes model and tests the model with the other 40% of the data.
	  * @param entries RDD of Feature Entries
	  * @return a Naive Bayes Model trained on the data
	  */
	def run(entries: RDD[FeatureEntry]): NaiveBayesModel = {
		val data = entries.map(_.labeledPoint())
		val Array(training, test) = data.randomSplit(Array(0.6, 0.4))
		val model = NaiveBayes.train(training, 1.0)
		val numCorrect = test
			.map(p => (model.predict(p.features), p.label))
			.filter(x => x._1 == x._2)
			.count
		val accuracy = 1.0 * numCorrect / test.count()
		println(s"accuracy of the model: $accuracy")
		model
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Classifier Training")
		val sc = new SparkContext(conf)
		val entries = sc.cassandraTable[FeatureEntry](keyspace, tablename)
		val model = run(entries)
		model.save(sc, s"wikipedia_naivebayes_model_${System.currentTimeMillis()}")
		sc.stop()
	}
}
