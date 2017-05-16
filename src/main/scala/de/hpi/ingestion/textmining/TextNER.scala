package de.hpi.ingestion.textmining

import de.hpi.ingestion.dataimport.wikipedia.models.WikipediaEntry
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models.{OffsetToken, ParsedWikipediaEntry}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.implicits.CollectionImplicits._

/**
  * todo
  */
object TextNER extends SparkJob {
	appName = "Text NER"
	val keyspace = "wikidumps"
	val tablename = "parsedwikipedia"
	val outputTablename = "namedentities"

	// $COVERAGE-OFF$
	/**
	  * Loads Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val wikipedia = sc.cassandraTable[WikipediaEntry](keyspace, tablename)
		List(wikipedia).toAnyRDD()
	}

	/**
	  * todo
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[OffsetToken]()
			.head
			.saveToCassandra(keyspace, outputTablename)
	}

	// $COVERAGE-ON$

	/**
	  * todo
	  * @param text
	  * @return
	  */
	def findNamedEntities(text: String): List[OffsetToken] = {
		List(OffsetToken("lorem", 0, 0))
	}

	/**
	  * todo
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val texts = input.head.asInstanceOf[RDD[ParsedWikipediaEntry]]
			.map(_.getText())
		val namedEntities = texts.map(text => findNamedEntities(text))
		List(namedEntities).toAnyRDD()
	}
}
