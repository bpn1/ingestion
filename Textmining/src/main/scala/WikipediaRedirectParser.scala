import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import scala.util.matching.Regex
import info.bliki.wiki.model.WikiModel
import WikipediaTextparser._
import org.jsoup.Jsoup
import com.datastax.spark.connector._
import WikiClasses._

object WikipediaRedirectParser {
	val tablename = "parsedwikipedia"
	val inputTablename = "wikipedia"
	val keyspace = "wikidumps"

	def parseRedirect(title: String, html: String): ParsedWikipediaEntry = {
		val parsedEntry = ParsedWikipediaEntry(title, Option(""), null)
		val doc = Jsoup.parse(html)
		val text = doc.body.text.replaceAll("(?i)((\\AWEITERLEITUNG)|(\\AREDIRECT))", "REDIRECT")
		parsedEntry.setText(text)
		parsedEntry.links = extractRedirect(html, text)
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
		sc.stop()
	}
}
