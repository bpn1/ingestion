package de.hpi.ingestion.deduplication

import java.util.UUID
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.deduplication.models.FeatureEntry
import de.hpi.ingestion.implicits.CollectionImplicits._

/**
  * Job for calculation of feature entries
  */
object FeatureCalculation extends SparkJob {
	appName = "Feature calculation"
	val keyspace = "evaluation"
	val dbpedia = "subject_dbpedia"
	val wikidata = "subject_wikidata"
	val goldStandard = "goldstandard"
	val features = "sim_measure_features"

	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val dbpedia = sc.cassandraTable[Subject](keyspace, dbpedia)
		val wikidata = sc.cassandraTable[Subject](keyspace, wikidata)
		val goldStandard = sc.cassandraTable[(UUID, UUID)](keyspace, goldStandard)
		List(dbpedia, wikidata).toAnyRDD() ::: List(goldStandard).toAnyRDD()
	}

	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[FeatureEntry]()
			.head
			.saveToCassandra(keyspace, features)
	}

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val inputSubjects = input.fromAnyRDD[Subject]()
		val dbpedia = inputSubjects.head
		val wikidata = inputSubjects(1)
		val goldStandard = input.fromAnyRDD[(UUID, UUID)]().head

		Nil
	}
}
