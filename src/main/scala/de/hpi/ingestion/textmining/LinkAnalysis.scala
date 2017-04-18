package de.hpi.ingestion.textmining

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.dataimport.wikipedia.models.WikipediaEntry

object LinkAnalysis {
	val keyspace = "wikidumps"
	val inputRawTablename = "wikipedia"
	val inputParsedTablename = "parsedwikipedia"
	val outputAliasToPagesTablename = "wikipedialinks"
	val outputPageToAliasesTablename = "wikipediapages"

	def getAllPages(sc: SparkContext): RDD[String] = {
		sc.cassandraTable[WikipediaEntry](keyspace, inputRawTablename)
			.map(_.title)
	}

	def removeDeadLinks(links: RDD[Alias], allPages: RDD[String]): RDD[Alias] = {
		links
			.flatMap { link =>
				link.pages
					.map(page => (link.alias, page._1, page._2))
			}
			.keyBy { case (alias, pageName, count) => pageName }
			.join(allPages.map(entry => (entry, entry)).keyBy(_._1)) // ugly, but working
			.map { case (pageName, (link, doublePageName)) => link }
			.map { case (alias, pageName, count) => (alias, Map((pageName, count))) }
			.reduceByKey(_ ++ _)
			.map { case (alias, links) => Alias(alias, links) }
	}

	def removeDeadPages(pages: RDD[Page], allPages: RDD[String]): RDD[Page] = {
		pages
			.keyBy(_.page)
			.join(allPages.map(entry => (entry, entry)).keyBy(_._1))
			.map { case (pageName, (page, doublePageName)) => page }
	}

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

	def probabilityLinkDirectsToPage(alias: Alias, pageName: String): Double = {
		val totalReferences = alias
			.pages
			.foldLeft(0)(_ + _._2)
		val references = alias.pages.getOrElse(pageName, 0)
		references / totalReferences
	}

	def fillAliasToPagesTable(parsedWikiRDD: RDD[ParsedWikipediaEntry], sc: SparkContext): Unit = {
		removeDeadLinks(groupByAliases(parsedWikiRDD), getAllPages(sc))
			.saveToCassandra(keyspace, outputAliasToPagesTablename)
	}

	def fillPageToAliasesTable(parsedWikiRDD: RDD[ParsedWikipediaEntry], sc: SparkContext): Unit = {
		removeDeadPages(groupByPageNames(parsedWikiRDD), getAllPages(sc))
			.saveToCassandra(keyspace, outputPageToAliasesTablename)
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Link Analysis")
			.set("spark.cassandra.connection.host", "odin01")
		val sc = new SparkContext(conf)

		val parsedWikiRDD = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputParsedTablename)
		fillPageToAliasesTable(parsedWikiRDD, sc)
		fillAliasToPagesTable(parsedWikiRDD, sc)
		sc.stop
	}
}
