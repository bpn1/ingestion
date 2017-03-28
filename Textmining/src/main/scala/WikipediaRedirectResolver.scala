import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import scala.util.matching.Regex
import info.bliki.wiki.model.WikiModel
import WikipediaTextparser._
import org.jsoup.Jsoup
import com.datastax.spark.connector._
import scala.collection.mutable
import WikiClasses._

object WikipediaRedirectResolver {
	val tablename = "parsedwikipedia"
	val keyspace = "wikidumps"

	def checkRedirectPage(firstLink: Link, title: String): Boolean = {
		val categoryRegex = new Regex("Kategorie:")
		categoryRegex.findFirstIn(firstLink.page).isEmpty && title != firstLink.page
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
			.set("spark.cassandra.connection.host", "odin01")
		val sc = new SparkContext(conf)
		val dict = mutable.Map[String, String]()

		val redirectRegex = new Regex("REDIRECT[^\\s]")
		val dataRDD = sc.cassandraTable[ParsedWikipediaEntry](keyspace, tablename).cache
		dataRDD
			.map { entry =>
				val text = entry.text match {
					case Some(t) => t
					case None => ""
				}
				(entry.title, entry.links, text)
			}.filter { case (title, links, text: String) =>
				redirectRegex.findFirstIn(text).isDefined
			}.collect()
			.foreach { case (title, links, text) =>
				if (checkRedirectPage(links.head, title)) {
					dict(title) = links.head.page
				}
			}
		val dictBroadcast = sc.broadcast(dict)
		dataRDD
			.mapPartitions({ entryRDD =>
				val localDict = dictBroadcast.value
				entryRDD.map { entry =>
					entry.links.foreach { link =>
						cleanLinkOfRedirects(link, localDict)
					}
					entry
				}
			}, true)
			.saveToCassandra(keyspace, tablename)
		sc.stop()
	}
}
