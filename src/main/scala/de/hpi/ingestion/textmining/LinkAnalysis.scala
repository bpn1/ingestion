package de.hpi.ingestion.textmining

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.implicits.CollectionImplicits._

/**
  * Groups link Aliases by page names and vice versa without counting dead links.
  */
object LinkAnalysis extends SparkJob {
	appName = "Link Analysis"
	configFile = "textmining.xml"
	cassandraSaveQueries += "TRUNCATE TABLE wikidumps.wikipedialinks"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val articles = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		List(articles).toAnyRDD()
	}

	/**
	  * Saves the grouped aliases and links to the Cassandra.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		val aliases = output.head.asInstanceOf[RDD[Alias]]
		val pages = output(1).asInstanceOf[RDD[Page]]
		aliases.saveToCassandra(settings("keyspace"), settings("linkTable"), SomeColumns("alias", "pages"))
		pages.saveToCassandra(settings("keyspace"), settings("pageTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Extracts links from Wikipedia articles that point to an existing page.
	  *
	  * @param articles all Wikipedia articles
	  * @return valid links
	  */
	def extractValidLinks(articles: RDD[ParsedWikipediaEntry]): RDD[Link] = {
		val joinableExistingPages = articles.map(article => (article.title, article.title))
		articles
			.flatMap(_.allLinks().map(l => (l.page, l)))
			.join(joinableExistingPages)
			.values
			.map { case (link, page) => link }
	}

	/**
	  * Creates RDD of Aliases with corresponding pages.
	  *
	  * @param links links of Wikipedia articles
	  * @return RDD of Aliases
	  */
	def groupByAliases(links: RDD[Link]): RDD[Alias] = {
		val aliasData = links.map(link => (link.alias, List(link.page)))
		groupLinks(aliasData).map(t => Alias(t._1, t._2))
	}

	/**
	  * Creates RDD of pages with corresponding Aliases.
	  *
	  * @param links links of Wikipedia articles
	  * @return RDD of pages
	  */
	def groupByPageNames(links: RDD[Link]): RDD[Page] = {
		val pageData = links.map(link => (link.page, List(link.alias)))
		groupLinks(pageData).map(Page.tupled)
	}

	/**
	  * Groups link data on the key, counts the number of elements per key and filters entries with an empty field.
	  * @param links RDD containing the link data in a predefined format
	  * @return RDD containing the counted and filtered link data
	  */
	def groupLinks(links: RDD[(String, List[String])]): RDD[(String, Map[String, Int])] = {
		links
			.reduceByKey(_ ++ _)
			.map { case (key, valueList) =>
				val countedValues = valueList
					.filter(_.nonEmpty)
					.countElements()
				(key, countedValues)
			}.filter(t => t._1.nonEmpty && t._2.nonEmpty)
	}

	/**
	  * Groups the links once on the aliases and once on the pages. Also removes dead links (no corresponding page).
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val articles = input.fromAnyRDD[ParsedWikipediaEntry]().head
		val validLinks = extractValidLinks(articles)
		val aliases = groupByAliases(validLinks)
		val pages = groupByPageNames(validLinks)
		List(aliases).toAnyRDD() ++ List(pages).toAnyRDD()
	}
}
