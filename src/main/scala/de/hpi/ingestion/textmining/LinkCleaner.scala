package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models.ParsedWikipediaEntry
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._

/**
  * Removes dead links that do not point to an existing page.
  */
object LinkCleaner extends SparkJob {
	appName = "Link Cleaner"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val articles = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		List(articles).toAnyRDD()
	}

	/**
	  * Saves cleaned Parsed Wikipedia entries to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[ParsedWikipediaEntry]()
			.head
			.saveToCassandra(settings("keyspace"), settings("parsedWikiTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Groups existing page names by the article names in which their links occur.
	  *
	  * @param articles      all Wikipedia articles
	  * @param existingPages names of all existing Wikipedia articles
	  * @return Wikipedia page names and their corresponding valid referenced page names
	  */
	def groupValidPages(
		articles: RDD[ParsedWikipediaEntry],
		existingPages: RDD[String]
	): RDD[(String, Set[String])] = {
		articles
			.flatMap(article => article.allLinks().map(link => (link.page, article.title)))
			.join(existingPages.map(page => Tuple2(page, page)))
			.values
			.map { case (title, page) => (title, Set(page)) }
			.reduceByKey(_ ++ _)
	}

	/**
	  * Filters links of Wikipedia articles and discards links that do not point to a valid page.
	  *
	  * @param articles   all Wikipedia articles
	  * @param validPages Wikipedia page names and their corresponding valid referenced page names
	  * @return Wikipedia articles without links to non-existing pages
	  */
	def filterArticleLinks(
		articles: RDD[ParsedWikipediaEntry],
		validPages: RDD[(String, Set[String])]
	): RDD[ParsedWikipediaEntry] = {
		articles
			.map(article => (article.title, article))
			.join(validPages)
			.values
			.map { case (article, links) =>
				article.filterLinks(link => links.contains(link.page))
				article
			}
	}

	/**
	  * Removes dead links that do not point to an existing page from the Parsed Wikipedia entries.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val articles = input.fromAnyRDD[ParsedWikipediaEntry]().head
		val existingPages = articles.map(_.title)
		val validPages = groupValidPages(articles, existingPages)
		List(filterArticleLinks(articles, validPages)).toAnyRDD()
	}
}
