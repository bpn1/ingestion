package de.hpi.ingestion.textmining.preprocessing

import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Exports a sample of `FeatureEntries` as .csv file for eventual evaluation.
  * (This is just for evaluation and no necessary part of the pipeline.)
  */
object FeatureCsvExport extends SparkJob {
	appName = "FeatureCsvExport"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Reads the feature entries entities from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val features = sc.cassandraTable[FeatureEntry](settings("keyspace"), settings("secondOrderFeatureTable"))
		List(features).toAnyRDD()
	}

	/**
	  * Saves the feature csv string to the HDFS.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		val featuresCsvString = output.fromAnyRDD[String]().head
		val timestamp = System.currentTimeMillis / 1000
		featuresCsvString.saveAsTextFile(s"features_$timestamp.csv")
	}
	// $COVERAGE-ON$

	/**
	  * Exports a sample of `FeatureEntries` as .csv file for eventual evaluation.
	  * (This is just for evaluation and no necessary part of the pipeline.)
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val features = input.fromAnyRDD[FeatureEntry]().head
		val featuresString = features
			.filter(_.cosine_sim.value >= 0)
			.take(settings("featuresSampleSize").toInt)
			.map { f =>
				val List(article, alias, entity) = List(f.article, f.alias, f.entity).map(_.replaceAll(",", "#"))
				List(article, f.offset, alias, entity, f.correct, f.link_score,
					f.entity_score.value, f.entity_score.rank, f.entity_score.delta_top, f.entity_score.delta_successor,
					f.cosine_sim.value, f.cosine_sim.rank, f.cosine_sim.delta_top, f.cosine_sim.delta_successor
				).mkString(",")
			}.mkString("\n")
		val csvHeader = "article,offset,alias,entity,correct,link_score,entity_score,entity_score_rank," +
			"entity_score_dtop,entity_score_dsucc,cosine_sim,cosine_sim_rank,cosine_sim_dtop,cosine_sim_dsucc\n"
		val featuresCsvString = csvHeader + featuresString
		List(sc.parallelize(List(featuresCsvString))).toAnyRDD()
	}
}
