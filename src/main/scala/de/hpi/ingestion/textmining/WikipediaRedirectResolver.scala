package de.hpi.ingestion.textmining

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import scala.collection.mutable
import de.hpi.ingestion.textmining.models._

object WikipediaRedirectResolver {
	val tablename = "parsedwikipedia"
	val keyspace = "wikidumps"

	def isValidRedirect(link: Link, title: String): Boolean = {
		title != link.page
	}

	def cleanLinkOfRedirects(link: Link, dict: Map[String, String]): Link = {
		var lookupSet = Set[String]()
		var lookupResult = dict.getOrElse(link.page, link.page)
		while (dict.contains(lookupResult) && !lookupSet.contains(lookupResult)) {
			lookupSet += lookupResult
			lookupResult = dict.getOrElse(lookupResult, lookupResult)
		}
		link.page = lookupResult
		link
	}

	def resolveRedirects(entry: ParsedWikipediaEntry, dict: Map[String, String]): ParsedWikipediaEntry = {
		entry.allLinks().foreach { link =>
			cleanLinkOfRedirects(link, dict)
		}
		entry
	}

	def main(args: Array[String]) {
		val conf = new SparkConf()
			.setAppName("WikipediaRedirectResolver")
		val sc = new SparkContext(conf)
		val dict = mutable.Map[String, String]()

		val dataRDD = sc.cassandraTable[ParsedWikipediaEntry](keyspace, tablename).cache
		dataRDD
			.filter(WikipediaTextParser.isRedirectPage)
			.map(entry => (entry.title, entry.textlinks))
			.collect()
			.foreach { case (title, links) =>
				links
					.filter(isValidRedirect(_, title))
					.foreach(link => dict(title) = link.page)
			}

		val dictBroadcast = sc.broadcast(dict)
		dataRDD
			.mapPartitions({ entryRDD =>
				val localDict = dictBroadcast.value
				entryRDD.map(entry => resolveRedirects(entry, localDict.toMap))
			}, true)
			.saveToCassandra(keyspace, tablename)
		sc.stop()
	}
}
