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
	configFile = "textmining.xml"
	cassandraSaveQueries += s"TRUNCATE TABLE ${settings("keyspace")}.${settings("linkTable")}"
	val reduceFlag = "toReduced"

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
		val aliases = output.head.asInstanceOf[RDD[Alias]].map(alias => (alias.alias, alias.pages))
		val pages = output(1).asInstanceOf[RDD[Page]].map(page => (page.page, page.aliases))
		aliases.saveToCassandra(settings("keyspace"), settings("linkTable"), SomeColumns("alias", "pages"))
		pages.saveToCassandra(settings("keyspace"), settings("pageTable"), SomeColumns("page", "aliases"))
	}
	// $COVERAGE-ON$

	/**
	  * Extracts links from Wikipedia articles that point to an existing page.
	  * @param articles all Wikipedia articles
	  * @param toReduced whether or not the reduced link columns are used
	  * @return valid links
	  */
	def extractValidLinks(articles: RDD[ParsedWikipediaEntry], toReduced: Boolean = false): RDD[Link] = {
		val joinableExistingPages = articles.map(article => (article.title, article.title))
		articles
			.flatMap { entry =>
				val links = if(toReduced) entry.reducedLinks() else entry.allLinks()
				links.map(l => (l.page, l))
			}.join(joinableExistingPages)
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
		groupLinks(aliasData).map { case (alias, pages) => Alias(alias, pages) }
	}

	/**
	  * Creates RDD of pages with corresponding Aliases.
	  *
	  * @param links links of Wikipedia articles
	  * @return RDD of pages
	  */
	def groupByPageNames(links: RDD[Link]): RDD[Page] = {
		val pageData = links.map(link => (link.page, List(link.alias)))
		groupLinks(pageData).map { case (page, aliases) => Page(page, aliases) }
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
		val toReduced = args.headOption.contains(reduceFlag)
		val validLinks = extractValidLinks(articles, toReduced)
		val aliases = groupByAliases(validLinks)
		val pages = groupByPageNames(validLinks)
		List(aliases).toAnyRDD() ++ List(pages).toAnyRDD()
	}
}
