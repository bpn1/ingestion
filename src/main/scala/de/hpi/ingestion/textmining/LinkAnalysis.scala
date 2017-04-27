package de.hpi.ingestion.textmining

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.textmining.models._

/**
  * Groups link Aliases by page names and vice versa without counting dead links.
  */
object LinkAnalysis {
	val keyspace = "wikidumps"
	val inputRawTablename = "wikipedia"
	val inputParsedTablename = "parsedwikipedia"
	val outputAliasToPagesTablename = "wikipedialinks"
	val outputPageToAliasesTablename = "wikipediapages"

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
	  * Extracts links from Wikipedia articles that point to an existing page.
	  *
	  * @param articles all Wikipedia articles
	  * @return valid links
	  */
	def extractValidLinks(articles: RDD[ParsedWikipediaEntry]): RDD[Link] = {
		val joinableExistingPages = articles.map(article => (article.title, article.title))
		articles
			.flatMap(_.allLinks())
			.keyBy(_.page)
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
		links
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
	  * Groups link Aliases by page names and vice versa. Also removes dead links (no corresponding page).
	  *
	  * @param articles all Wikipedia articles
	  * @return grouped Aliases and page names
	  */
	def run(articles: RDD[ParsedWikipediaEntry]): (RDD[Alias], RDD[Page]) = {
		val aliases = groupByAliases(extractValidLinks(articles))
		val pages = groupByPageNames(articles)
		(aliases, pages)
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("Link Analysis")
		val sc = new SparkContext(conf)

		val articles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputParsedTablename)
		val (aliases, pages) = run(articles)
		aliases.saveToCassandra(keyspace, outputAliasToPagesTablename, SomeColumns("alias", "pages"))
		pages.saveToCassandra(keyspace, outputPageToAliasesTablename)

		sc.stop
	}
}
