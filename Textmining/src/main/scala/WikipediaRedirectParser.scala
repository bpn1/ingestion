import org.apache.spark.{SparkConf, SparkContext}
import scala.util.matching.Regex
import WikipediaTextparser._
import org.jsoup.Jsoup
import com.datastax.spark.connector._
import WikiClasses._

object WikipediaRedirectParser {
	val tablename = "parsedwikipedia"
	val inputTablename = "wikipedia"
	val keyspace = "wikidumps"
	val redirectText = "REDIRECT "

	def parseRedirect(title: String, html: String): ParsedWikipediaEntry = {
		val parsedEntry = ParsedWikipediaEntry(title, Option(""), null, null)
		val doc = Jsoup.parse(html)
		val text = doc.body.text.replaceAll("(?i)((\\AWEITERLEITUNG)|(\\AREDIRECT))", redirectText)
		parsedEntry.setText(text)
		parsedEntry.textlinks = extractRedirect(html, text)
		parsedEntry
	}

	def main(args: Array[String]) {
		val conf = new SparkConf()
			.setAppName("WikipediaRedirectParser")
			.set("spark.cassandra.connection.host", "odin01")
		val sc = new SparkContext(conf)

		val redirectRegex = new Regex("(?i)(#((Weiterleitung)|(redirect))\\s?:?)")
		val replaceRegex = new Regex("(?i)((Weiterleitung:?)|(redirect))\\s?:?")
		val wikiRDD = sc.cassandraTable[WikipediaEntry](keyspace, inputTablename)
			.map(entry => (entry.title, entry.getText))
			.filter { case (title, text) =>
				redirectRegex.findFirstIn(text).isDefined
			}
			.map(entry => (entry._1, replaceRegex.replaceAllIn(entry._2, "WEITERLEITUNG")))
			.map(entry => (entry._1, wikipediaToHtml(entry._2)))
			.map(entry => parseRedirect(entry._1, entry._2))
			.saveToCassandra(keyspace, tablename)

			// .keyBy(_.title)
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
//			.map { entry =>
//				val regex = new Regex("(?i)(redirect)\\s?")
//			(entry._2._1._1, regex.replaceAllIn(entry._2._1._2, "WEITERLEITUNG"))
//			}
		// 	.map(entry => (entry._1, entry._2, wikipediaToHtml(entry._2)))
		// 	.foreach(println)

		sc.stop()
	}
}
