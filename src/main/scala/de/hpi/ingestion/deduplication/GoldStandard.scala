package de.hpi.ingestion.deduplication

import java.util.UUID

import de.hpi.ingestion.datalake.models.Subject
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._

object GoldStandard extends SparkJob {
	appName = "GoldStandard_v1.0"
	configFile = "goldstandard.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads DBpedia and Wikidata from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val dbpedia = sc.cassandraTable[Subject](settings("keyspaceDBpediaTable"), settings("dBpediaTable"))
		val wikidata = sc.cassandraTable[Subject](settings("keyspaceWikiDataTable"), settings("wikiDataTable"))
		List(dbpedia, wikidata).toAnyRDD()
	}

	/**
	  * Saves joined DBpedia and Wikidata to {@output} table in keyspace {@keyspace}.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[(UUID, UUID)]()
			.head
			.saveToCassandra(settings("keyspaceGoldStandardTable"), settings("goldStandardTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Joins DBpedia and Wikidata.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val List(dbpedia, wikidata) = input.fromAnyRDD[Subject]()
		val results = join(dbpedia, wikidata)
		List(results).toAnyRDD()
	}

	/**
	  * Keys Subjects by a property value
	  * @param rdd Subjects
	  * @param property Property to key by
	  * @return RDD containing Subjects keyed by a value
	  */
	def keyBySingleProperty(rdd: RDD[Subject], property: String): RDD[(String, Subject)] = {
		rdd
			.filter(_.properties.contains(property))
			.map { subject =>
				val keyOpt = subject.properties(property).headOption
				(keyOpt.getOrElse(""), subject)
			}
	}

	/**
	  * Join DBpedia and WikiData together on a property.
	  * @param property The property to be joined on.
	  * @param dbpedia Subjects from DBpedia
	  * @param wikidata Subjects from WikiData
	  * @return RDD containing pairs of UUIDs
	  */
	def joinBySingleProperty(property: String, dbpedia: RDD[Subject], wikidata: RDD[Subject]): RDD[(UUID, UUID)] = {
		val propertyKeyDBpedia = keyBySingleProperty(dbpedia, property)
		val propertyKeyWikiData = keyBySingleProperty(wikidata, property)
		propertyKeyDBpedia
			.join(propertyKeyWikiData)
			.values
			.map { case (dbpediaSubject, wikidataSubject) =>
				(dbpediaSubject.id, wikidataSubject.id)
			}
	}

	/**
	  * Join DBpedia and WikiData together based on ids
	  * @param dbpedia Subjects from DBpedia
	  * @param wikidata Subjects from WikiData
	  * @return RDD containing pairs of UUIDs
	  */
	def join(dbpedia: RDD[Subject], wikidata: RDD[Subject]): RDD[(UUID, UUID)] = {
		val dbpediaIdJoined = joinBySingleProperty("id_dbpedia", dbpedia, wikidata)
		val wikidataIdJoined = joinBySingleProperty("id_wikidata", dbpedia, wikidata)
		dbpediaIdJoined.union(wikidataIdJoined).distinct
	}
}
