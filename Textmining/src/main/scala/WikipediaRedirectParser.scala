import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import scala.util.matching.Regex
import info.bliki.wiki.model.WikiModel
import WikipediaTextparser._
import org.jsoup.Jsoup
import com.datastax.spark.connector._



object WikipediaRedirectParser {
	val tablename = "parsedwikipedia"
	val inputTablename = "wikipedia"
	val keyspace = "wikidumps"
	case class Wikientry(title: String, text: Option[String])


	def parseRedirect(title: String, html: String): ParsedWikipediaEntry = {
		val parsedEntry = new ParsedWikipediaEntry(title, Option(""), null)
		val doc = Jsoup.parse(html)
		val text = doc.body.text.replaceAll("(?i)((\\AWEITERLEITUNG)|(\\AREDIRECT))", "REDIRECT")
		parsedEntry.setText(text)
		parsedEntry.links = extractRedirect(html)
		return parsedEntry

	}

	def main(args: Array[String]) {
		val conf = new SparkConf()
			.setAppName("WikipediaRedirectParser")
			.set("spark.cassandra.connection.host", "172.20.21.11")
		val sc = new SparkContext(conf)

		val redirectRegex = new Regex("(?i)(#((Weiterleitung)|(redirect))\\s?:?)")
		val replaceRegex = new Regex("(?i)((Weiterleitung:?)|(redirect))\\s?:?")
		val wikiRDD = sc.cassandraTable[Wikientry](keyspace, inputTablename)
			.map{ entry =>
				val text = entry.text match {
					case Some(t) => t
					case None => ""
				}
				(entry.title, text)
			}
			.filter{ case (title, text: String) =>
				redirectRegex.findFirstIn(text) != None
			}
			.map(entry => (entry._1, replaceRegex.replaceAllIn(entry._2, "WEITERLEITUNG")))
			.map(entry => (entry._1, wikipediaToHtml(entry._2)))
			.map(entry => parseRedirect(entry._1, entry._2))
			.saveToCassandra(keyspace, tablename)
		// 	.keyBy(_.title)
		//
		// val badLinks = sc.cassandraTable[ParsedWikipediaEntry](keyspace, tablename)
		// 	.filter(_.links.isEmpty)
		// 	.keyBy(_.title)
		//
		// badLinks.rightOuterJoin(wikiRDD)
		// 	.filter(_._2._1 == None)
		// 	.collect
		// 	.take(1000)
		// 	.foreach(println)

		// val parsedWikipediaRDD = sc
		// 	.cassandraTable[ParsedWikipediaEntry](keyspace, tablename)
		// 	.filter(_.links.isEmpty)
		// 	.keyBy(_.title)
		//
		// val joinedRDD = wikiRDD
		// 	.join(parsedWikipediaRDD)
		// 	.take(100)
		// .map(entry => (entry._2._1._1, new Regex("([rR][eE][dD][iI][rR][eE][cC][tT])\\s?").replaceAllIn(entry._2._1._2, "WEITERLEITUNG")))
		// 	.map(entry => (entry._1, entry._2, wikipediaToHtml(entry._2)))
		// 	.foreach(println)

		sc.stop()
	}
}
