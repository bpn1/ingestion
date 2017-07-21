package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.textmining.models._
import com.datastax.spark.connector._

object ReducedLinkAnalysis extends SparkJob {
	appName = "Reduced Link Analysis"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		LinkAnalysis.load(sc, args)
	}

	/**
	  * Saves the grouped aliases and links to the Cassandra.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		val aliases = output.head.asInstanceOf[RDD[Alias]].map(alias => (alias.alias, alias.pages))
		val pages = output(1).asInstanceOf[RDD[Page]].map(page => (page.page, page.aliases))
		aliases.saveToCassandra(settings("keyspace"), settings("linkTable"), SomeColumns("alias", "pagesreduced"))
		pages.saveToCassandra(settings("keyspace"), settings("pageTable"), SomeColumns("page", "aliasesreduced"))
	}
	// $COVERAGE-ON$

	/**
	  * Groups the links once on the aliases and once on the pages, removes dead links (no corresponding page) and saves
	  * them to the reduced links columns.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		LinkAnalysis.run(input, sc, Array(LinkAnalysis.reduceFlag))
	}
}
