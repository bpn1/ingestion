package de.hpi.ingestion.textmining

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.implicits.TupleImplicits._

/**
  * Resolves all redirects in Parsed Wikipedia entries by replacing them with the pages they point to
  * and writing the redirects to a cassandra table.
  */
object RedirectResolver extends SparkJob {
	appName = "Redirect Resolver"
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

		val articles = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		val redirects = sc.cassandraTable[Redirect](settings("keyspace"), settings("redirectTable"))
		List(articles).toAnyRDD() ++ List(redirects).toAnyRDD()
	}

	/**
	  * Saves Parsed Wikipedia entries with resolved redirects
	  * and the redirects themselves to the Cassandra.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.head
			.asInstanceOf[RDD[ParsedWikipediaEntry]]
			.saveToCassandra(settings("keyspace"), settings("parsedWikiTable"))
		val redirects = output(1).asInstanceOf[RDD[Redirect]]
		if(!redirects.isEmpty()) redirects.saveToCassandra(settings("keyspace"), settings("redirectTable"))
	}

	// $COVERAGE-ON$

	/**
	  * Resolves all redirects in the links of an articles. It does so by replacing the target page with the page the
	  * redirect points to.
	  *
	  * @param entry Parsed Wikipedia Entry containing the links that will be cleaned
	  * @param dict  Map containing all redirects
	  * @return Parsed Wikipedia Entry with no redirect pages as target of a link
	  */
	def resolveRedirects(entry: ParsedWikipediaEntry, dict: Map[String, String]): ParsedWikipediaEntry = {
		entry.allLinks().foreach(link => link.page = dict.getOrElse(link.page, link.page))
		entry
	}

	/**
	  * Builds Map of redirects from redirect articles.
	  *
	  * @param articles RDD of Parsed Wikipedia Articles
	  * @return Map containing the redirects in the form redirect page -> target page
	  */
	def buildRedirectDict(articles: RDD[ParsedWikipediaEntry]): Map[String, String] = {
		articles
			.filter(TextParser.isRedirectPage)
			.map(entry => (entry.title, entry.textlinks.headOption.map(_.page)))
			.filter(t => t._2.isDefined && t._1 != t._2.get)
			.map(_.map(identity, _.get))
			.collect()
			.toMap
	}

	/**
	  * Resolves transitive redirects by replacing the target page with the transitive target page. Also removes
	  * all reflexive entries.
	  *
	  * @param redirectMap Map containing the redirects that will be cleaned
	  * @return Map containing the cleaned redirects
	  */
	def resolveTransitiveRedirects(redirectMap: Map[String, String]): Map[String, String] = {
		var resolvedRedirects = redirectMap
		var resolvableEntries = Map[String, String]()
		var visited = Set[String]()
		do {
			resolvedRedirects ++= resolvableEntries.map(_.map(identity, resolvedRedirects))
			resolvedRedirects = resolvedRedirects.filter(t => t._1 != t._2)

			visited ++= resolvableEntries.values
			resolvableEntries = resolvedRedirects
				.filter(t => resolvedRedirects.contains(t._2))
				.filter(t => !visited.contains(t._2))
		} while(resolvableEntries.nonEmpty)
		resolvedRedirects
	}

	/**
	  * Resolves redirects for every Parsed Wikipedia entry. It checks if redirects where already found,
	  * if not it finds all redirects, resolves transitive redirects
	  * and then replaces links to redirect pages with links to the page the redirect directs to.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val articles = input.head.asInstanceOf[RDD[ParsedWikipediaEntry]]
		var redirects = input(1).asInstanceOf[RDD[Redirect]]
			.map(Redirect.unapply(_).get)
			.collect
			.toMap
		val saveRedirectsToCassandra = redirects.isEmpty
		if(saveRedirectsToCassandra) {
			val redirectMap = buildRedirectDict(articles)
			redirects = resolveTransitiveRedirects(redirectMap)
		}
		val dictBroadcast = sc.broadcast(redirects)
		val resolvedArticles = articles
			.mapPartitions({ entryPartition =>
				val localDict = dictBroadcast.value
				entryPartition.map(entry => resolveRedirects(entry, localDict))
			}, true)
		val redirectsList = if(saveRedirectsToCassandra) redirects.map(Redirect.tupled).toList else Nil
		List(resolvedArticles).toAnyRDD() ++ List(sc.parallelize(redirectsList)).toAnyRDD()
	}
}
