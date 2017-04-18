package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models.{Alias, Page, ParsedWikipediaEntry}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.wikipedia.models.WikipediaEntry
import de.hpi.ingestion.textmining.WikipediaLinkAnalysis.{inputRawTablename, keyspace}

/**
  * Removes dead links that do not point to an existing page
  */
object LinkCleaner {
	val keyspace = "wikidumps"
	val operatingTable = "parsedwikipedia"

	def getAllPages(sc: SparkContext): RDD[String] = {
		sc.cassandraTable[WikipediaEntry](keyspace, inputRawTablename)
			.map(_.title)
	}

	//	def removeDeadLinks(links: RDD[Alias], allPages: RDD[String]): RDD[Alias] = {
	//		links
	//			.flatMap { link =>
	//				link.pages
	//					.map(page => (link.alias, page._1, page._2))
	//			}
	//			.keyBy { case (alias, pageName, count) => pageName }
	//			.join(allPages.map(entry => (entry, entry)).keyBy(_._1)) // ugly, but working
	//			.map { case (pageName, (link, doublePageName)) => link }
	//			.map { case (alias, pageName, count) => (alias, Map((pageName, count))) }
	//			.reduceByKey(_ ++ _)
	//			.map { case (alias, links) => Alias(alias, links) }
	//	}
	//
	//	def removeDeadPages(pages: RDD[Page], allPages: RDD[String]): RDD[Page] = {
	//		pages
	//			.keyBy(_.page)
	//			.join(allPages.map(entry => (entry, entry)).keyBy(_._1))
	//			.map { case (pageName, (page, doublePageName)) => page }
	//	}

	def run(articles: RDD[ParsedWikipediaEntry]): RDD[ParsedWikipediaEntry] = {
		articles
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Link Cleaner")
		val sc = new SparkContext(conf)

		val articles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, operatingTable)
		run(articles)
			.saveToCassandra(keyspace, operatingTable)

		sc.stop
	}
}
