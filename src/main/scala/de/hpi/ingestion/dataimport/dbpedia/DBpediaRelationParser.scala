package de.hpi.ingestion.dataimport.dbpedia

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.dbpedia.models.Relation
import de.hpi.ingestion.dataimport.dbpedia.DBpediaImport.{dbpediaToCleanedTriples, getPrefixesFromFile}
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Extracts all relations from english dbpedia and translates the english title to the german ones.
  */
object DBpediaRelationParser extends SparkJob {
	appName = "DBpediaRelationParser"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads triples of relations and english to german translation from ttl files.
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
	  * Saves relations to cassandra.
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
	  * Parses only English to German translations from DBpedia triples.
	  *
	  * @param labels RDD of triples containing the same as relation
	  * @return Map from english wikipedia titles to their german counterparts
	  */
	def getGermanLabels(labels: RDD[String], prefixes: List[(String, String)]): Map[String, String] = {
		dbpediaToCleanedTriples(labels, prefixes)
			.filter { case List(subj, rel, obj) => rel.startsWith("owl:") && obj.startsWith("dbpedia-de:") }
			.map(_.map(_.replaceAll("dbo:|dbpedia-de:|dbr:", "")))
			.map { case List(subj, rel, obj) => subj -> obj }
			.collect
			.toMap
	}

	/**
	  * Extracts all relations from English DBpedia and translates the English title to the German ones.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val List(dbpedia, labels) = input.fromAnyRDD[String]()
		val prefixes = getPrefixesFromFile()
		val germanLabels = getGermanLabels(labels, prefixes)
		val gerLabelsBroadcast = sc.broadcast(germanLabels)
		val relations = dbpediaToCleanedTriples(dbpedia, prefixes, "dbo:|dbr:")
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
