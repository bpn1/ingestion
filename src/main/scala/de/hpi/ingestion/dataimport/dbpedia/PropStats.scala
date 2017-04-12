package de.hpi.ingestion.dataimport.dbpedia

import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models.Subject
import org.apache.spark.{SparkConf, SparkContext}

object PropStats {

	val keyspace = "datalake"

	def propertyStats(sc: SparkContext, keyspace: String, tablename: String) = {
		val input = sc.cassandraTable[Subject](keyspace, tablename)
		val maxSize = input.count()
		input
			.flatMap(_.properties.keys.toList.map(key => (key, 1)))
			.reduceByKey(_ + _)
			.sortBy(_._2, false)
			.map(tup => tup._1 + "\t" + (tup._2.toDouble / maxSize))
			.saveAsTextFile(s"propertyStat_${tablename}_${System.currentTimeMillis()}")
	}

	def main(args: Array[String]) {
		val conf = new SparkConf()
			.setAppName("PropStats")
		val sc = new SparkContext(conf)
		propertyStats(sc, keyspace, "subject_temp")
		propertyStats(sc, keyspace, "subject_implisense_temp")
		propertyStats(sc, keyspace, "subject")

		sc.stop()
	}
}
