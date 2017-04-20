package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models.{Page, ParsedWikipediaEntry}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

/**
  * Removes dead links that do not point to an existing page.
  */
object LinkCleaner {
	val keyspace = "wikidumps"
	val operatingTable = "parsedwikipedia"

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
	  * Removes dead links that do not point to an existing page.
	  *
	  * @param articles all Wikipedia articles
	  * @return Wikipedia articles without links to non-existing pages
	  */
	def run(articles: RDD[ParsedWikipediaEntry]): RDD[ParsedWikipediaEntry] = {
		val existingPages = articles.map(_.title)
		val validPages = groupValidPages(articles, existingPages)
		filterArticleLinks(articles, validPages)
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("Link Cleaner")
		val sc = new SparkContext(conf)

		val articles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, operatingTable)
		run(articles).saveToCassandra(keyspace, operatingTable)

		sc.stop
	}
}
