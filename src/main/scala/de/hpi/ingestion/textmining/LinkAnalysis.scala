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
	  * @param links links of Wikipedia articles
	  * @return RDD of pages
	  */
	def groupByPageNames(links: RDD[Link]): RDD[Page] = {
		links
			.map(link => (link.page, List(link.alias)))
			.reduceByKey(_ ++ _)
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
		val validLinks = extractValidLinks(articles)
		val aliases = groupByAliases(validLinks)
		val pages = groupByPageNames(validLinks)
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
