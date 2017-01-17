import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.matching.Regex
import info.bliki.wiki.model.WikiModel
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import org.jsoup.select.Elements
import java.net.URLDecoder

object WikipediaTextparser {
	val keyspace = "wikidumps"
	val tablename = "wikipedia"

	case class WikipediaReadEntry(
		title: String,
		var text: Option[String],
		var refs: Map[String, String])

	case class WikipediaEntry(
		title: String,
		var text: String,
		var refs: Map[String, String])

	def wikipediaToHtml(wikipediaMarkup: String): String = {
		val html = wikipediaMarkup.replaceAll("\\\\n", "\n")
		WikiModel.toHtml(removeWikiMarkup(html))
	}

	def parseHtml(entry : (WikipediaEntry, String)): WikipediaEntry = {
		val redirectRegex = new Regex("(\\AWEITERLEITUNG)|(\\AREDIRECT)")
		if(redirectRegex.findFirstIn(Jsoup.parse(entry._2).body.text) != None) {
			val wikiEntry = entry._1
			wikiEntry.refs = getLinks(Jsoup.parse(entry._2))
			wikiEntry.text = "REDIRECT"
			return wikiEntry
		}

		val document = removeTags(entry._2)
		val wikiEntry = entry._1
		wikiEntry.refs = getLinks(document)
		wikiEntry.text = addHtmlEncodingLine(document.toString)
		wikiEntry
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
			escaped = character == '\\'
		}
		cleanText
	}

	def removeTags(html: String): Document = {
		val doc = Jsoup.parse(html)
		val documentContent = doc
			.select("p")
			.filter(_.getElementsByAttributeValueStarting("title", "Datei:").size == 0)
			.map { element =>
				element.getElementsByClass("reference").remove
				element.select("small").remove
				element
			}
		val htmltext = documentContent.map(_.toString).mkString("\n")
			.replaceAll("&lt;(/|)gallery&gt;", "") // removes gallery tags
			.replaceAll("<p></p>", "") // removes empty p tags
		Jsoup.parse(htmltext)
	}

	def getLinks(html: Document): Map[String, String] = {
		val links = mutable.ListBuffer[(String, String)]()
		for(anchor <- html.select("a")) {
			val source = anchor.text
			var target = anchor.attr("href")
			target = target
				.replaceAll("%(?![0-9a-fA-F]{2})", "%25")
         		.replaceAll("\\+", "%2B")
			try {
				target = URLDecoder.decode(target, "UTF-8")
					.replaceAll("_", " ")
			} catch {
				case e: java.lang.IllegalArgumentException =>
					println(s"IllegalArgumentException for: $target")
					target = anchor.attr("href")
			}
			if(target.length == 0) {
				println(s"target length ist 0: $target")
			} else if(target.charAt(0) == '/') {
				target = target.substring(1)
			}
			anchor.attr("href", target)
			links += Tuple2[String,String](source, target)
		}
		links.toMap
	}

	def addHtmlEncodingLine(html: String): String = {
		val encodingLine = "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />"
		encodingLine + "\n" + html
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Textparser")
			.set("spark.cassandra.connection.host", "172.20.21.11")
		val sc = new SparkContext(conf)

		val wikipedia = sc.cassandraTable[WikipediaReadEntry](keyspace, tablename)
		wikipedia
			.map { entry =>
				val text = entry.text match {
					case Some(string) => string
					case None => ""
				}
				WikipediaEntry(entry.title, text, entry.refs)
			}.map(entry => (entry, wikipediaToHtml(entry.text)))
			.map(parseHtml)
			.saveToCassandra(keyspace, tablename)
		sc.stop
	}
}
