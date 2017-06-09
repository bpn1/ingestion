package de.hpi.ingestion.textmining

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.implicits.CollectionImplicits._

/**
  * Counts Alias occurrences and merges them into previously extracted Aliases with their corresponding pages.
  */
object AliasCounter extends SparkJob {
	appName = "Alias Counter"
	configFile = "textmining.xml"
	val maximumAliasLength = 1000

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia and Aliases from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val articles = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		List(articles).toAnyRDD()
	}

	/**
	  * Saves Alias occurrence counts to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[(String, Option[Int], Option[Int])]()
			.head
			.saveToCassandra(
				settings("keyspace"),
				settings("linkTable"),
				SomeColumns("alias", "linkoccurrences", "totaloccurrences"))
	}
	// $COVERAGE-ON$

	/**
	  * Extracts list of link and general Alias occurrences for an article.
	  *
	  * @param entry article from which the Aliases will be extracted
	  * @return list of Aliases each with an occurrence set to 1
	  */
	def extractAliasList(entry: ParsedWikipediaEntry): List[Alias] = {
		val links = entry.allLinks().map(_.alias)
		val linkCount = links.map(Alias(_, linkoccurrences = Option(1), totaloccurrences = Option(1)))
		val aliasCount = entry.foundaliases
			.diff(linkCount.map(_.alias))
			.map(Alias(_, linkoccurrences = Option(0), totaloccurrences = Option(1)))
		linkCount ++ aliasCount
	}

	/**
	  * Reduces two AliasCounters of the same alias.
	  *
	  * @param alias1 first Alias with the same alias as alias2
	  * @param alias2 second Alias with the same alias as alias1
	  * @return Alias with summed link and total occurrences
	  */
	def aliasReduction(alias1: Alias, alias2: Alias): Alias = {
		Alias(
			alias1.alias,
			alias1.pages ++ alias2.pages,
			alias1.pagesreduced ++ alias2.pagesreduced,
			Option(alias1.linkoccurrences.get + alias2.linkoccurrences.get),
			Option(alias1.totaloccurrences.get + alias2.totaloccurrences.get)
		)
	}

	/**
	  * Counts link and total occurrences of link aliases in Wikipedia articles.
	  *
	  * @param articles RDD containing parsed wikipedia articles
	  * @return RDD containing alias counts for links and total occurrences
	  */
	def countAliases(articles: RDD[ParsedWikipediaEntry]): RDD[Alias] = {
		articles
			.flatMap(extractAliasList)
			.map(alias => (alias.alias, alias))
			.reduceByKey(aliasReduction)
			.map(_._2)
			.filter(_.alias.nonEmpty)
	}

	/**
	  * Counts alias occurrences and merges them into previously extracted aliases with their corresponding pages.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val articles = input.fromAnyRDD[ParsedWikipediaEntry]().head
		val countTuples = countAliases(articles)
			.filter(_.alias.length <= maximumAliasLength)
			.map(entry => (entry.alias, entry.linkoccurrences, entry.totaloccurrences))
		List(countTuples).toAnyRDD()
	}
}
