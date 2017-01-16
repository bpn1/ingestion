import org.sweble.wikitext._
import org.sweble.wikitext.engine._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.sweble.wikitext.engine.utils._
import org.sweble.wikitext.engine.nodes._
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.matching.Regex
import info.bliki.wiki.model.WikiModel
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import org.jsoup.select.Elements

object WikipediaTextparser {
	val keyspace = "wikidumps"
	val tablename = "wikipedia"

	case class WikipediaEntry(
								 title: String,
								 var text: String,
								 var links: List[Map[String, String]])

	def wikipediaToHtml(entry: WikipediaEntry): WikipediaEntry = {
		var cleanText = entry.text.replaceAll("\\\\n", "\n")
		cleanText = removeWikiMarkup(cleanText)
		cleanText = WikiModel.toHtml(cleanText)
		cleanText = removeTables(cleanText)
		entry.text = cleanText
		entry
	}

	def removeWikiMarkup(text: String): String = {
		var cleanText = ""
		var depth = 0
		var escaped = false

		for (character <- text) {
			if (!escaped && character == '{')
				depth += 1
			else if (!escaped && character == '}' && depth > 0)
				depth -= 1
			else if (depth == 0)
				cleanText += character

			if (character == '\\')
				escaped = true
			else
				escaped = false
		}
		cleanText
	}

	def removeTables(html: String): String = {
		val doc = Jsoup.parse(html)
		doc.select("table").remove() // does not work with <div> tags, because there are tables outside them
		doc.toString()
	}


	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("VersionDiff")
			.set("spark.cassandra.connection.host", "172.20.21.11")
		val sc = new SparkContext(conf)

		val wikipedia = sc.cassandraTable[WikipediaEntry](keyspace, tablename)
		/*
		wikipedia
            .map(wikipediaToHtml)
			.saveToCassandra(keyspace, tablename)
		*/
		sc.stop
	}
}
