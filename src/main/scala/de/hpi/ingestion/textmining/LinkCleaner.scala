package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models.{Page, ParsedWikipediaEntry}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

/**
  * Removes dead links that do not point to an existing page
  */
object LinkCleaner {
	val keyspace = "wikidumps"
	val inputPagesTable = "wikipediapages"
	val operatingTable = "parsedwikipedia"

	/**
	  * Groups existing page names by the article names in which their links occur.
	  * @param articles all Wikipedia articles
	  * @param existingPages names of all existing Wikipedia articles
	  * @return
	  */
	def groupValidPages(
		articles: RDD[ParsedWikipediaEntry],
		existingPages: RDD[String]
	): RDD[(String, Set[String])] = {
		articles
			.flatMap { article =>
				article
					.allLinks()
					.map(link => (link.page, article.title))
			}
			.join(existingPages.map(page => (page, page)).keyBy(_._1))
			.map { case (page, (title, (page1, page2))) => (title, page) }
			.groupByKey
			.mapValues(_.toSet)
	}

	/**
	  *
	  * @param articles
	  * @param validLinks
	  * @return
	  */
	def filterArticleLinks(
		articles: RDD[ParsedWikipediaEntry],
		validLinks: RDD[(String, Set[String])]
	): RDD[ParsedWikipediaEntry] = {
		articles
			.map(article => (article.title, article))
			.join(validLinks)
			.map { case (title, (article, links)) =>
				article.filterLinks(link => links.contains(link.page))
				article
			}
	}

	/**
	  *
	  * @param articles
	  * @param existingPages
	  * @return
	  */
	def run(
		articles: RDD[ParsedWikipediaEntry],
		existingPages: RDD[String]
	): RDD[ParsedWikipediaEntry] = {
		val validPages = groupValidPages(articles, existingPages)
		filterArticleLinks(articles, validPages)
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Link Cleaner")
		val sc = new SparkContext(conf)

		val articles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, operatingTable)
		val existingPages = sc.cassandraTable[Page](keyspace, inputPagesTable)
			.map(_.page)

		run(articles, existingPages)
			.saveToCassandra(keyspace, operatingTable)

		sc.stop
	}
}
