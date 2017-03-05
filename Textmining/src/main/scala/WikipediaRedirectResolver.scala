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

	def main(args: Array[String]) {
		val conf = new SparkConf()
			.setAppName("WikipediaRedirectResolver")
			.set("spark.cassandra.connection.host", "odin01")
		val sc = new SparkContext(conf)

		var dict = mutable.Map[String, String]()

		val redirectRegex = new Regex("(?i)((Weiterleitung:?)|(redirect))\\s?:?")
		val dataRDD = sc.cassandraTable[ParsedWikipediaEntry](keyspace, tablename).cache
		dataRDD
			.map{ entry =>
				val text = entry.text match {
					case Some(t) => t
					case None => ""
				}
				(entry.title, entry.links, text)
			}
			.filter{ case (title, links, text: String) =>
				redirectRegex.findFirstIn(text).isDefined
			}
			.collect()
			.foreach{ case (title, links, text) =>
				if (links.size == 1 && title != links.head.page) {
					dict(title) = links.head.page
				}
			}
		val dictBroadcast = sc.broadcast(dict)
		dataRDD
			.mapPartitions({ entryRDD =>
				val localDict = dictBroadcast.value
				entryRDD.map { entry =>
					entry.links.foreach { link =>
						var lookupSet = Set[String]()
						var lookupResult = localDict.getOrElse(link.page, link.page)
						while(dict.contains(lookupResult) || lookupSet.contains(lookupResult)) {
							lookupSet += lookupResult
							lookupResult = localDict.getOrElse(lookupResult, lookupResult)
							println(s"looking up $lookupResult")
							println(s"looking up ${entry.title} ${entry.links}")
						}
						link.page = lookupResult
					}
					entry
				}
			}, true)
			.saveToCassandra(keyspace, tablename)
		sc.stop()
	}
}
