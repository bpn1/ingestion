package de.hpi.ingestion.textmining

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.implicits.TupleImplicits._

object RedirectResolver {
	val tablename = "parsedwikipedia"
	val keyspace = "wikidumps"

	/**
	  * Resolves all redirects in the links of an articles. It does so by replacing the target page with the page the
	  * redirect points to.
	  * @param entry Parsed Wikipedia Entry containing the links that will be cleaned
	  * @param dict Map containing all redirects
	  * @return Parsed Wikipedia Entry with no redirect pages as target of a link
	  */
	def resolveRedirects(entry: ParsedWikipediaEntry, dict: Map[String, String]): ParsedWikipediaEntry = {
		entry.allLinks().foreach(link => link.page = dict.getOrElse(link.page, link.page))
		entry
	}

	/**
	  * Builds Map of redirects from redirect articles.
	  * @param articles RDD of Parsed Wikipedia Articles
	  * @return Map containing the redirects in the form redirect page -> target page
	  */
	def buildRedirectDict(articles: RDD[ParsedWikipediaEntry]): Map[String, String] = {
		articles
			.filter(WikipediaTextParser.isRedirectPage)
			.map(entry => (entry.title, entry.textlinks.headOption.map(_.page)))
			.filter(t => t._2.isDefined && t._1 != t._2.get)
			.map(_.map(identity, _.get))
			.collect()
			.toMap
	}

	/**
	  * Resolves transitive redirects by replacing the target page with the transitive target page. Also removes
	  * all reflexive entries.
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
	  * Finds all redirects, resolves transitive redirects and then replaces links to redirect pages with links to the
	  * page the redirect directs to.
	  * @param articles RDD of Parsed Wikipedia Entries
	  * @param sc Spark Context used to broadcast the redirect dictionary
	  * @return RDD of parsed Wikipedia Entries with resolved redirect links
	  */
	def run(articles: RDD[ParsedWikipediaEntry], sc: SparkContext): RDD[ParsedWikipediaEntry] = {
		val redirectMap = buildRedirectDict(articles)
		val cleanedRedirectMap = resolveTransitiveRedirects(redirectMap)
		val dictBroadcast = sc.broadcast(cleanedRedirectMap)

		articles
			.mapPartitions({ entryPartition =>
				val localDict = dictBroadcast.value
				entryPartition.map(entry => resolveRedirects(entry, localDict))
			}, true)
	}

	def main(args: Array[String]) {
		val conf = new SparkConf()
			.setAppName("Redirect Resolver")
		val sc = new SparkContext(conf)

		val articles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, tablename).cache
		run(articles, sc).saveToCassandra(keyspace, tablename)
		sc.stop()
	}
}
