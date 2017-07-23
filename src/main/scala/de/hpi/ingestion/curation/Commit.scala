package de.hpi.ingestion.curation

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.JSONParser
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.implicits.TupleImplicits._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue

object Commit extends SparkJob with JSONParser[(List[Subject], List[Subject], List[Subject])] {
	appName = "Commit"

	/**
		* Loads a number of input RDDs used in the job.
		*
		* @param sc   Spark Context used to load the RDDs
		* @param args arguments of the program
		* @return List of RDDs containing the data processed in the job.
		*/
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		println(args(0))
		val subjects = sc.cassandraTable[Subject]("datalake", "subject")
		List(subjects).toAnyRDD()
	}

	/**
		* Executes the data processing of this job and produces the output data.
		*
		* @param input List of RDDs containing the input data
		* @param sc    Spark Context used to e.g. broadcast variables
		* @param args  arguments of the program
		* @return List of RDDs containing the output data
		*/
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		println(args(0))
		val subjects = input.take(1).fromAnyRDD[Subject]()
		subjects.toAnyRDD()
	}

	/**
		* Saves the output data to e.g. the Cassandra or the HDFS.
		*
		* @param output List of RDDs containing the output of the job
		* @param sc     Spark Context used to connect to the Cassandra or the HDFS
		* @param args   arguments of the program
		*/
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
	}

	/**
		* Extracts the JSON data from a JSON object to an Entity.
		*
		* @param json JSON-Object containing the data
		* @return Entity containing the parsed data
		*/
	override def fillEntityValues(json: JsValue): (List[Subject], List[Subject], List[Subject]) = {
		val entity = fillSimpleValues(json)
		entity.aliases = extractAliases(json)

		val claims = extractMap(json, List("claims"))
		entity.data = claims.mapValues { content =>
			content
				.as[List[JsValue]]
				.flatMap(extractClaimValues)
		}
		entity
	}
}
