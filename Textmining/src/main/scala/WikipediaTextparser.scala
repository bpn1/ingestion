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

	def removeWikiMarkup(wikitext: String): String = {
		var cleanText = ""
		var depth = 0
		var escaped = false
		for (character <- wikitext) {
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

	def extractInfobox(wikitext: String): String = {
		var infoboxText = ""
		var infoboxRegex = new Regex("\\{\\{Infobox ")
		// cannot check if first '{' is escaped because page may begin with infobox
		var startIndex = -1

		infoboxRegex
			.findAllIn(wikitext)
			.matchData
			.foreach { regexMatch =>
				assert(startIndex == -1) // there must not be more than one infobox
				startIndex = regexMatch.start
			}
		if (startIndex == -1) {
			return infoboxText
		}

		var depth = 0
		var escaped = false
		for (i <- startIndex until wikitext.length) {
			val character = wikitext(i)
			if (!escaped && character == '{')
				depth += 1
			else if (!escaped && character == '}' && depth > 0)
				depth -= 1
			infoboxText += character
			if (depth == 0)
				return infoboxText
			escaped = character == '\\'
		}

		assert(false) // infobox should end earlier
		infoboxText
	}

	def linkMatchToLink(linkMatch: scala.util.matching.Regex.Match): Link = {
		val page = linkMatch.group(1)
		if(page.startsWith("Datei:"))
			return null

		var alias = ""
		if (linkMatch.groupCount > 2)
			alias = linkMatch.group(2)
		if (alias != null)
			alias = alias.stripPrefix("|")
		if (alias == null || alias.isEmpty)
			alias = page
		
		Link(alias, page, WikipediaTextparser.infoboxOffset)
	}

	def extractInfoboxLinks(wikitext: String): List[Link] = {
		val linkList = mutable.ListBuffer[Link]()
		val infoboxText = extractInfobox(wikitext)
		val linkRegex = new Regex("\\[\\[(.*?)(\\|(.*?))?\\]\\]")
		linkRegex
			.findAllIn(infoboxText)
			.matchData
			.foreach { linkMatch =>
				val link = linkMatchToLink(linkMatch)
				if(link != null)
					linkList += link
			}
		linkList.toList
	}

	def parseWikipediaEntry(entry: WikipediaEntry): ParsedWikipediaEntry = {
		val html = wikipediaToHtml(entry.text)
		var parsedEntry = parseHtml(entry, html)
		val infoboxLinks = extractInfoboxLinks(entry.text)
		parsedEntry.links ++= infoboxLinks
		parsedEntry
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
			}.map(parseWikipediaEntry)
			.saveToCassandra(keyspace, outputTablename)
		sc.stop
	}
}
