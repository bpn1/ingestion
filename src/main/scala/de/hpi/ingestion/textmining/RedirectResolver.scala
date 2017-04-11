package de.hpi.ingestion.textmining

import org.apache.spark.{SparkConf, SparkContext}
import scala.util.matching.Regex
import com.datastax.spark.connector._
import scala.collection.mutable
import de.hpi.ingestion.textmining.models._

object WikipediaRedirectResolver {
	val tablename = "parsedwikipedia"
	val keyspace = "wikidumps"

	def isValidRedirect(link: Link, title: String): Boolean = {
		title != link.page
	}

	def cleanLinkOfRedirects(link: Link, dict: mutable.Map[String, String]): Link = {
		var lookupSet = Set[String]()
		var lookupResult = dict.getOrElse(link.page, link.page)
		while(dict.contains(lookupResult) && !lookupSet.contains(lookupResult)) {
			lookupSet += lookupResult
			lookupResult = dict.getOrElse(lookupResult, lookupResult)
		}
		link.page = lookupResult
		link
	}

	def main(args: Array[String]) {
		val conf = new SparkConf()
			.setAppName("WikipediaRedirectResolver")
		val sc = new SparkContext(conf)
		val dict = mutable.Map[String, String]()

		val redirectRegex = new Regex("REDIRECT[^\\s]")
		val dataRDD = sc.cassandraTable[ParsedWikipediaEntry](keyspace, tablename).cache
		dataRDD
			.filter(entry => redirectRegex.findFirstIn(entry.getText()).isDefined)
			.map(entry => (entry.title, entry.textlinks))
			.collect()
			.foreach { case (title, links) =>
				links.foreach { link =>
					if (isValidRedirect(link, title)) {
						dict(title) = link.page
					}
				}
			}

		val dictBroadcast = sc.broadcast(dict)
		dataRDD
			.mapPartitions({ entryRDD =>
				val localDict = dictBroadcast.value
				entryRDD.map { entry =>
					entry.textlinks = entry.textlinks.map(link =>
						cleanLinkOfRedirects(link, localDict))
					entry.templatelinks = entry.templatelinks.map(link =>
						cleanLinkOfRedirects(link, localDict))
					entry.categorylinks = entry.categorylinks.map(link =>
						cleanLinkOfRedirects(link, localDict))
					entry.disambiguationlinks = entry.disambiguationlinks.map(link =>
						cleanLinkOfRedirects(link, localDict))
					entry
				}
			}, true)
			.saveToCassandra(keyspace, tablename)
		sc.stop()
	}
}
