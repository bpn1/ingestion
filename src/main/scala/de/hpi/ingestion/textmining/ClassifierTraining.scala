package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._

object ClassifierTraining extends SparkJob {
	appName = "Classifier Training"
	val keyspace = "wikidumps"
	val tablename = "featureentries"

	// $COVERAGE-OFF$
	/**
	  * Loads Feature entries from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val entries = sc.cassandraTable[FeatureEntry](keyspace, tablename)
		List(entries).toAnyRDD()
	}

	/**
	  * Saves Naive Bayes Model to the HDFS.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[NaiveBayesModel]()
			.head
			.collect
			.head
			.save(sc, s"wikipedia_naivebayes_model_${System.currentTimeMillis()}")
	}
	// $COVERAGE-ON$

	/**
	  * Uses 60% of the entries to train a Naive Bayes model and tests the model with the other 40% of the data.
	  * Example code: https://spark.apache.org/docs/latest/mllib-naive-bayes.html
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val entries = input.fromAnyRDD[FeatureEntry]().head
		val data = entries.map(_.labeledPoint())
		val Array(training, test) = data.randomSplit(Array(0.6, 0.4))
		val model = NaiveBayes.train(training, 1.0)
		val numCorrect = test
			.map(p => (model.predict(p.features), p.label))
			.filter(x => x._1 == x._2)
			.count
		val accuracy = 1.0 * numCorrect / test.count()
		println(s"accuracy of the model: $accuracy")
		List(sc.parallelize(Seq(model))).toAnyRDD()
	}
}
