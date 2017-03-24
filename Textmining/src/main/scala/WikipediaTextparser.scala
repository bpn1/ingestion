import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.matching.Regex
import info.bliki.wiki.model.WikiModel
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element, Node, TextNode}
import java.net.URLDecoder
import scala.collection.mutable.ListBuffer
import WikiClasses._

object WikipediaTextparser {
	val keyspace = "wikidumps"
	val tablename = "wikipedia"
	val outputTablename = "parsedwikipedia"
	val templateOffset = -1
	val redirectText = "REDIRECT "

	def wikipediaToHtml(wikipediaMarkup: String): String = {
		val html = wikipediaMarkup.replaceAll("\\\\n", "\n")
		WikiModel.toHtml(removeWikiMarkup(html))
	}

	def checkRedirect(html: String): Boolean = {
		val redirectRegex = new Regex("\\AWEITERLEITUNG")
		val searchText = Jsoup.parse(html).body.text
		redirectRegex.findFirstIn(searchText).isDefined
	}

	def extractRedirect(html: String, text: String): List[Link] = {
		val anchorList = Jsoup.parse(html).select("a")
		val linksList = mutable.ListBuffer[Link]()
		val redirectRegex = new Regex("REDIRECT( )*")
		val categoryRegex = new Regex("Kategorie:")
		val redirectOffset = (redirectRegex.findFirstIn(text) match {
			case Some(x) => x
			case None => redirectText
		}).length
		for (anchor <- anchorList) {
			var offset = text.indexOfSlice(anchor.text)
			if (categoryRegex.findFirstIn(anchor.text).isEmpty) {
				offset = redirectOffset
			}
			linksList += Link(anchor.text, parseUrl(anchor.attr("href")), offset)
		}
		linksList.toList
	}

	def parseHtml(entry: (WikipediaEntry, String)): ParsedWikipediaEntry = {
		val parsedEntry = ParsedWikipediaEntry(entry._1.title, Option(""), null)
		if (checkRedirect(entry._2)) {
			val doc = Jsoup.parse(entry._2)
			val text = doc.body.text.replaceAll("\\AWEITERLEITUNG", redirectText)
			parsedEntry.setText(text)
			parsedEntry.links = extractRedirect(entry._2, text)
			return parsedEntry
		}

		val document = removeTags(entry._2)
		val outputTuple = extractLinks(document.body)
		parsedEntry.setText(outputTuple._1)
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

		if (children.isEmpty) {
			return ("", linksList.toList)
		}

		if (children(0).isInstanceOf[TextNode]) {
			startIndex = 1
			var firstChildText = children(0).asInstanceOf[TextNode].text
			while (firstChildText.charAt(0) != body.text.charAt(0)) {
				firstChildText = firstChildText.substring(1)
				if (firstChildText.length == 0) {
					return ("", linksList.toList)
				}
			}
			offset += firstChildText.length
		}

		for (element <- children.slice(startIndex, children.length)) {
			element match {
				case t: Element =>
					val target = parseUrl(t.attr("href"))
					val source = if (t.text == "") target else t.text
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
			if (!escaped && character == '{') {
				depth += 1
			} else if (!escaped && character == '}' && depth > 0) {
				depth -= 1
			} else if (depth == 0) {
				cleanText += character
			}
			escaped = character == '\\'
		}
		cleanText
	}

	def extractTemplate(wikitext: String, startIndex: Int): String = {
		var templateText = ""
		var depth = 0
		var escaped = false
		for (i <- startIndex until wikitext.length) {
			val character = wikitext(i)
			if (!escaped && character == '{') {
				depth += 1
			} else if (!escaped && character == '}' && depth > 0) {
				depth -= 1
			}
			templateText += character
			if (depth == 0) {
				return templateText
			}
			escaped = character == '\\'
		}

		println("[Textparser WARN]	Template has no end!")
		templateText
	}

	def extractAllTemplates(wikitext: String): String = {
		var templateText = ""
		var templateRegex = new Regex("\\{\\{")
		// cannot check if first '{' is escaped because page may begin with template
		var startIndices = new ListBuffer[Int]()

		templateRegex
			.findAllIn(wikitext)
			.matchData
			.foreach { regexMatch =>
				startIndices += regexMatch.start
			}

		for (startIndex <- startIndices) {
			templateText += extractTemplate(wikitext, startIndex)
		}
		templateText
	}

	def linkMatchToLink(linkMatch: scala.util.matching.Regex.Match): Link = {
		val page = linkMatch.group(1)
		if (page.startsWith("Datei:")) {
			return null
		}

		var alias = ""
		if (linkMatch.groupCount > 2) {
			alias = linkMatch.group(2)
		}
		if (alias != null) {
		alias = alias.stripPrefix("|")
		}
		if (alias == null || alias.isEmpty) {
			alias = page
		}


		Link(alias, page, templateOffset)
	}

	def extractTemplateLinks(wikitext: String): List[Link] = {
		val linkList = mutable.ListBuffer[Link]()
		val templateText = extractAllTemplates(wikitext)
		val linkRegex = new Regex("\\[\\[(.*?)(\\|(.*?))?\\]\\]")
		linkRegex
			.findAllIn(templateText)
			.matchData
			.foreach { linkMatch =>
				val link = linkMatchToLink(linkMatch)
				if (link != null) {
					linkList += link
				}
			}
		linkList.toList
	}

	def parseWikipediaEntry(entry: WikipediaEntry): ParsedWikipediaEntry = {
		val html = wikipediaToHtml(entry.getText)
		var parsedEntry = parseHtml(entry, html)
		val templateLinks = extractTemplateLinks(entry.getText)
		parsedEntry.links ++= templateLinks
		parsedEntry
	}

	def cleanRedirects(entry: WikipediaEntry): WikipediaEntry = {
		val redirectRegex = "(?i)#(weiterleitung|redirect) ?(: ?)?"
		entry.setText(entry.getText.replaceAll(redirectRegex, "WEITERLEITUNG"))
		entry
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Text Parser")
			.set("spark.cassandra.connection.host", "odin01")
		val sc = new SparkContext(conf)

		val wikipedia = sc.cassandraTable[WikipediaEntry](keyspace, tablename)
		wikipedia
			.map(cleanRedirects)
			.map(parseWikipediaEntry)
			.saveToCassandra(keyspace, outputTablename)
		sc.stop
	}
}
