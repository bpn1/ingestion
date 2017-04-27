package de.hpi.ingestion.textmining

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.implicits.CollectionImplicits._

/**
  * Groups link Aliases by page names and vice versa without counting dead links.
  */
object LinkAnalysis extends SparkJob {
	appName = "Link Analysis"
	val keyspace = "wikidumps"
	val inputRawTablename = "wikipedia"
	val inputParsedTablename = "parsedwikipedia"
	val outputAliasToPagesTablename = "wikipedialinks"
	val outputPageToAliasesTablename = "wikipediapages"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val articles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputParsedTablename)
		List(articles).toAnyRDD()
	}

	/**
	  * Saves the grouped aliases and links to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		val aliases = output.head.asInstanceOf[RDD[Alias]]
		val pages = output(1).asInstanceOf[RDD[Page]]
		aliases.saveToCassandra(keyspace, outputAliasToPagesTablename, SomeColumns("alias", "pages"))
		pages.saveToCassandra(keyspace, outputPageToAliasesTablename)
	}
	// $COVERAGE-ON$

	/**
	  * Removes links that do not point to an existing page.
	  *
	  * @param links    links to be filtered
	  * @param allPages every page in Wikipedia
	  * @return filtered RDD of links
	  */
	def removeDeadLinks(links: RDD[Alias], allPages: RDD[String]): RDD[Alias] = {
		links
			.flatMap { link =>
				link.pages
					.map(page => (link.alias, page._1, page._2))
			}
			.keyBy { case (alias, pageName, count) => pageName }
			.join(allPages.map(entry => Tuple2(entry, entry)))
			.map { case (pageName, (link, doublePageName)) => link }
			.map { case (alias, pageName, count) => (alias, Map((pageName, count))) }
			.reduceByKey(_ ++ _)
			.map { case (alias, links) => Alias(alias, links) }
	}

	/**
	  * Removes non existing pages.
	  *
	  * @param pages    pages to be filtered
	  * @param allPages all existing pages
	  * @return filtered RDD of pages
	  */
	def removeDeadPages(pages: RDD[Page], allPages: RDD[String]): RDD[Page] = {
		pages
			.keyBy(_.page)
			.join(allPages.map(entry => Tuple2(entry, entry)))
			.map { case (pageName, (page, doublePageName)) => page }
	}

	/**
	  * Creates RDD of Aliases with corresponding pages.
	  *
	  * @param parsedWikipedia RDD of parsed Wikipedia entries to be processed
	  * @return RDD of Aliases
	  */
	def groupByAliases(parsedWikipedia: RDD[ParsedWikipediaEntry]): RDD[Alias] = {
		parsedWikipedia
			.flatMap(_.allLinks())
			.map(link => (link.alias, List(link.page)))
			.reduceByKey(_ ++ _)
			.map { case (alias, pageList) =>
				val pages = pageList
					.filter(_.nonEmpty)
					.groupBy(identity)
					.mapValues(_.size)
					.map(identity)
				Alias(alias, pages)
			}
			.filter(alias => alias.alias.nonEmpty && alias.pages.nonEmpty)
	}

	/**
	  * Creates RDD of pages with corresponding Aliases.
	  *
	  * @param parsedWikipedia RDD of parsed Wikipedia entries to be processed
	  * @return RDD of pages
	  */
	def groupByPageNames(parsedWikipedia: RDD[ParsedWikipediaEntry]): RDD[Page] = {
		parsedWikipedia
			.flatMap(_.allLinks())
			.map(link => (link.page, link.alias))
			.groupByKey
			.map { case (page, aliasList) =>
				val aliases = aliasList
					.filter(_.nonEmpty)
					.groupBy(identity)
					.mapValues(_.size)
					.map(identity)
				Page(page, aliases)
			}
			.filter(page => page.page.nonEmpty && page.aliases.nonEmpty)
	}

	/**
	  * Calculates probability for Alias pointing to page.
	  *
	  * @param alias    Alias to be processed
	  * @param pageName pagename that is being pointed to
	  * @return probability for Alias pointing to page
	  */
	def probabilityLinkDirectsToPage(alias: Alias, pageName: String): Double = {
		val totalReferences = alias
			.pages
			.foldLeft(0)(_ + _._2)
		val references = alias.pages.getOrElse(pageName, 0)
		references / totalReferences
	}

	/**
	  * Groups link Aliases by page names and vice versa. Also removes dead links (no corresponding page).
	  *
	  * @param articles all Wikipedia articles
	  * @return grouped Aliases and page names
	  */
	def run(articles: RDD[ParsedWikipediaEntry]): (RDD[Alias], RDD[Page]) = {
		val allPages = articles.map(_.title)
		val aliases = removeDeadLinks(groupByAliases(articles), allPages)
		val pages = removeDeadPages(groupByPageNames(articles), allPages)
		(aliases, pages)
	}

	/**
	  * Groups the links once on the aliases and once on the pages.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val articles = input.fromAnyRDD[ParsedWikipediaEntry]().head
		val allPages = articles.map(_.title)
		val aliases = removeDeadLinks(groupByAliases(articles), allPages)
		val pages = removeDeadPages(groupByPageNames(articles), allPages)
		List(aliases).toAnyRDD() ++ List(pages).toAnyRDD()
	}
}
