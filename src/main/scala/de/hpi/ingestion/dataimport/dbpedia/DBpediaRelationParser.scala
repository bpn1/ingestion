package de.hpi.ingestion.dataimport.dbpedia

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.dbpedia.models.Relation
import de.hpi.ingestion.dataimport.dbpedia.DBpediaImport.{cleanURL, tokenize}
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.io.Source
/**
  * Import-Job to import DBpedia Subjects into the staging table of the datalake.
  */
object DBpediaRelationParser extends SparkJob {
	appName = "DBpediaRelationParser"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * TODO
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val dbpedia = sc.textFile("mappingbased_objects_en.ttl")
		val labels = sc.textFile("interlanguage_links_en.ttl")
		List(dbpedia).toAnyRDD() ++ List(labels).toAnyRDD()
	}

	/**
	  * TODO
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[Relation]()
			.head
			.saveToCassandra(settings("keyspace"), settings("DBpediaRelationTable"))
	}

	// $COVERAGE-ON$

	/**
	  * TODO
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val dbpedia = input.fromAnyRDD[String]().head
		val labels = input.fromAnyRDD[String]().last

		val prefixFile = Source.fromURL(getClass.getResource("/prefixes.txt"))
		val prefixes = prefixFile.getLines.toList
			.map(_.trim.replaceAll("""[()]""", "").split(","))
			.map(pair => (pair(0), pair(1)))

		val relationtypes = Set(
			"owningCompany",
			"divison",
			"publisher",
			"parentCompany",
			"manufactorer",
			"subsidiary",
			"production",
			"equity",
			"assets",
			"assetUnderManagement",
			"successor",
			"predecessor"
		)
		val germanLabels = labels
			.map(tokenize)
			.map(_.map(listEntry => cleanURL(listEntry, prefixes)))
			.filter { case List(subj, rel, obj) => rel.startsWith("owl:") && obj.startsWith("dbpedia-de:") }
			.map(_.map(listEntry => listEntry.replaceAll("dbo:|dbpedia-de:|dbr:", "")))
			.map { case List(subj, rel, obj) => subj -> obj }
			.collect
			.toMap

		val gerLabelsBroadcast = sc.broadcast(germanLabels)
		val relations = dbpedia
			.map(tokenize)
			.map(_.map(listEntry => cleanURL(listEntry, prefixes)))
			.map(_.map(listEntry => listEntry.replaceAll("dbo:|dbr:", "")))
			.mapPartitions({ entryPartition =>
				val localGerLabels = gerLabelsBroadcast.value
				entryPartition.map { case List(subj, rel, obj) =>
					val gerSubj = localGerLabels.getOrElse(subj, subj)
					val gerObj = localGerLabels.getOrElse(obj, obj)
					Relation(gerSubj.replaceAll("_", " "), rel, gerObj.replaceAll("_", " "))
				}
			})

		List(relations).toAnyRDD()
	}
}
