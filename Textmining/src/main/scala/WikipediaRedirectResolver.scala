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
		val wikiRDD = sc.cassandraTable[ParsedWikipediaEntry](keyspace, tablename)
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
				if (links.size == 1) {
					dict(title) = links.head.page
				}
			}

		var noRedirectsRDD = sc.cassandraTable[ParsedWikipediaEntry](keyspace, tablename)
			.map{ entry =>
				var i = entry.links.size
				while (i > 0) {
					i = entry.links.size
					entry.links.foreach{ link =>
						if (dict.contains(link.page)) {
							link.page = dict(link.page)
						} else {
							i -= 1
						}
					}
				}
				entry
			}
			.saveToCassandra(keyspace, tablename)
		sc.stop()
	}
}
