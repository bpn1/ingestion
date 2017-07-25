package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import scala.tools.nsc.io.File

/**
  * Exports a sample of features as csv file for eventual evaluation.
  */
object FeatureCsvExport extends SparkJob {
	appName = "TagEntities"
	configFile = "wikidata_import.xml"

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
	  * Save nothing.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {}
	// $COVERAGE-ON$

	/**
	  * Exports a sample of features as csv file for eventual evaluation.
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
			.map(f => s"${f.article.replaceAll(",", "#")},${f.offset},${f.alias.replaceAll(",", "#")}," +
				s"${f.entity.replaceAll(",", "#")},${f.link_score},${f.entity_score.value},${f.entity_score.rank}," +
				s"${f.entity_score.delta_top},${f.entity_score.delta_successor},${f.cosine_sim.value}," +
				s"${f.cosine_sim.rank},${f.cosine_sim.delta_top},${f.cosine_sim.delta_successor},${f.correct}")
			.mkString("\n")
		val csvHeader = "article,offset,alias,entity,link_score,entity_score,entity_score_rank,entity_score_dtop," +
			"entity_score_dsuc,cosine_sim,cosine_sim_rank,cosine_sim_dtop,cosine_sim_dsuc,correct\n"
		val featuresCsv = csvHeader + featuresString
		File("features.csv").writeAll(featuresCsv)
		Nil
	}
}
