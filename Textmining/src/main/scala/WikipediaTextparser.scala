import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.matching.Regex
import info.bliki.wiki.model.WikiModel
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import org.jsoup.nodes.TextNode
import org.jsoup.select.Elements
import java.net.URLDecoder

object WikipediaTextparser {
	val keyspace = "wikidumps"
	val tablename = "wikipedia"
	val infoboxOffset: Int = -1

	val outputTablename = "parsedwikipedia"

	case class WikipediaReadEntry(title: String,
		var text: Option[String],
		var references: Map[String, List[String]])

	case class WikipediaEntry(title: String,
		var text: String,
		var references: Map[String, List[String]])

	case class ParsedWikipediaEntry(title: String,
		var text: String,
		var links: List[Link])

	case class Link(alias: String,
		var page: String,
		offset: Int)

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

	def wikipediaToHtml(wikipediaMarkup: String): String = {
		val html = wikipediaMarkup.replaceAll("\\\\n", "\n")
		WikiModel.toHtml(removeWikiMarkup(html))
	}

	def checkRedirect(html: String): Boolean = {
		val redirectRegex = new Regex("(\\AWEITERLEITUNG)|(\\AREDIRECT)")
		val searchText = Jsoup.parse(html).body.text
		redirectRegex.findFirstIn(searchText) != None
	}

	def extractRedirect(html: String): List[Link] = {
		val anchorList = Jsoup.parse(html).select("a")
		val linksList = mutable.ListBuffer[Link]()
		for (anchor <- anchorList) {
			linksList += Link(anchor.text, parseUrl(anchor.attr("href")), "REDIRECT ".length)
		}
		linksList.toList
	}

	def parseHtml(entry: (WikipediaEntry, String)): ParsedWikipediaEntry = {
		val parsedEntry = ParsedWikipediaEntry(entry._1.title, "", null)
		if (checkRedirect(entry._2)) {
			val doc = Jsoup.parse(entry._2)
			val text = doc.body.text.replaceAll("(\\AWEITERLEITUNG)|(\\AREDIRECT)", "REDIRECT")
			parsedEntry.text = text
			parsedEntry.links = extractRedirect(entry._2)
			return parsedEntry
		}
		val document = removeTags(entry._2)
		val outputTuple = extractLinks(document.body)
		parsedEntry.text = outputTuple._1
		parsedEntry.links = outputTuple._2
		parsedEntry
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

	def removeTags(html: String): Document = {
		val doc = Jsoup.parse(html)
		val documentContent = doc
			.select("p")
			.map { element =>
				element.getElementsByClass("reference").remove
				element.select("small").remove
				element.getElementsByAttributeValueStarting("title", "Datei:").remove
				element
			}
		val htmltext = documentContent
			.map(_.toString)
			.mkString("\n")
			.replaceAll("&lt;(/|)gallery&gt;", "") // removes gallery tags
			.replaceAll("<(|/)([^a])>", "") // remove every tag other than anchors
			.replaceAll("<(|/)([^a])>", "")
		Jsoup.parse(htmltext)
	}

	def parseUrl(url: String): String = {
		var cleanedUrl = url
			.replaceAll("%(?![0-9a-fA-F]{2})", "%25")
			.replaceAll("\\+", "%2B")
		try {
			cleanedUrl = URLDecoder.decode(cleanedUrl, "UTF-8")
				.replaceAll("_", " ")
		} catch {
			case e: java.lang.IllegalArgumentException =>
				println(s"IllegalArgumentException for: $cleanedUrl")
				cleanedUrl = url
		}
		cleanedUrl.replaceAll("\\A/", "")
	}

	def extractLinks(body: Element): Tuple2[String, List[Link]] = {
		val linksList = mutable.ListBuffer[Link]()
		var offset = 0
		var startIndex = 0
		val children = body.childNodes

		if(children.isEmpty) {
			return ("", linksList.toList)
		}

		if(children(0).isInstanceOf[TextNode]) {
			startIndex = 1
			var firstChildText = children(0).asInstanceOf[TextNode].text
			while(firstChildText.charAt(0) != body.text.charAt(0)) {
				firstChildText = firstChildText.substring(1)
				if(firstChildText.length == 0) {
					return ("", linksList.toList)
				}
			}
			offset += firstChildText.length
		}

		for(element <- children.slice(startIndex, children.length)) {
			element match {
				case t: Element =>
					val target = parseUrl(t.attr("href"))
					val source = if(t.text == "") target else t.text
					val link = Link(source, target, offset)
					linksList += link
					offset += t.text.length
				case t: TextNode =>
					offset += t.text.length
				case _ =>
			}
		}
		(body.text, linksList.toList)
	}

	def extractInfoboxLinks(wikitext: String): List[Link] = {
		val linkList = mutable.ListBuffer[Link]()
		linkList.toList
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
				WikipediaEntry(entry.title, text, entry.references)
			}.map(entry => (entry, wikipediaToHtml(entry.text), extractInfoboxLinks(entry.text)))
			.map { case (entry, html, infoboxLinks) =>
				val entry = parseHtml((entry, html))
				entry.links = entry.links ++ infoboxLinks
				entry
			}.saveToCassandra(keyspace, outputTablename)
		sc.stop
	}
}
