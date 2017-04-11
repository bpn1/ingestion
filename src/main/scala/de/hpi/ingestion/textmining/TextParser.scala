package de.hpi.ingestion.textmining

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import scala.collection.JavaConversions._
import scala.util.matching.Regex
import info.bliki.wiki.model.WikiModel
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element, TextNode}
import java.net.URLDecoder
import scala.collection.mutable.ListBuffer
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.dataimport.wikipedia.models.WikipediaEntry
import scala.io.Source

object WikipediaTextparser {
	val keyspace = "wikidumps"
	val tablename = "wikipedia"
	val outputTablename = "parsedwikipedia"
	val namespaceFile = "dewiki_namespaces"
	val categoryNamespace = "Kategorie:"
	val redirectText = "REDIRECT "
	val disambiguationTitleSuffix = " (Begriffsklärung)"

	def wikipediaToHtml(wikiMarkup: String): String = {
		val html = wikiMarkup.replaceAll("\\\\n", "\n")
		WikiModel.toHtml(removeWikiMarkup(html))
	}

	def isRedirectPage(html: String): Boolean = {
		val redirectRegex = new Regex("\\AWEITERLEITUNG")
		val searchText = Jsoup.parse(html).body.text
		redirectRegex.findFirstIn(searchText).isDefined
	}

	def extractRedirect(title: String, html: String, text: String): List[Link] = {
		val anchorList = Jsoup.parse(html).select("a")
		val linksList = anchorList
			.map(anchor => Link(title, parseUrl(anchor.attr("href"))))
			.toList
		cleanMetapageLinks(linksList)
	}

	def parseHtml(title: String, html: String): ParsedWikipediaEntry = {
		val parsedEntry = ParsedWikipediaEntry(title)
		val document = Jsoup.parse(html)
		if (isRedirectPage(html)) {
			val text = document.body.text.replaceAll("\\AWEITERLEITUNG", redirectText)
			parsedEntry.setText(text)
			parsedEntry.textlinks = extractRedirect(title, html, text)
		} else if (title.endsWith(disambiguationTitleSuffix)) {
			val outputTuple = extractDisambiguationLinks(title, document.body)
			parsedEntry.setText(outputTuple._1)
			parsedEntry.disambiguationlinks = outputTuple._2
		} else {
			val cleanedDocument = removeTags(document)
			val outputTuple = extractTextLinks(cleanedDocument.body)
			parsedEntry.setText(outputTuple._1)
			parsedEntry.textlinks = outputTuple._2
			parsedEntry.listlinks = extractListLinks(html)
		}
		extractCategoryLinks(parsedEntry)
	}

	def removeTags(document: Document): Document = {
		val documentContent = document
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

	def isDisambiguationPage(entry: WikipediaEntry): Boolean = {
		val disambiguationWikiMarkupSuffix = "{{Begriffsklärung}}"
		entry.getText().endsWith(disambiguationWikiMarkupSuffix)
	}

	def extractDisambiguationLinks(title: String, body: Element): (String, List[Link]) = {
		val linksList = ListBuffer[Link]()
		val cleanTitle = title.stripSuffix(disambiguationTitleSuffix)
		body.select("a")
			.foreach { anchor =>
				val target = parseUrl(anchor.attr("href"))
				val link = Link(cleanTitle, target)
				linksList += link
			}
		(body.text, linksList.toList)
	}

	def extractTextLinks(body: Element): (String, List[Link]) = {
		val linksList = ListBuffer[Link]()
		var offset = 0
		var startIndex = 0
		val children = body.childNodes

		if (children.isEmpty) {
			return ("", linksList.toList)
		}

		if (children.head.isInstanceOf[TextNode]) {
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
					//assert(t.tag.toString == "a") // This may fail for strange reasons.
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
		val cleanedLinks = cleanMetapageLinks(linksList.toList)
		(body.text, cleanedLinks)
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
		val templateRegex = new Regex("\\{\\{")
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
		Link(alias, page)
	}

	def extractTemplateLinks(wikitext: String): List[Link] = {
		val linkList = ListBuffer[Link]()
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
		val cleanedEntry = cleanRedirects(entry)
		var title = cleanedEntry.title
		if (isDisambiguationPage(cleanedEntry) && !title.endsWith(disambiguationTitleSuffix)) {
			title += disambiguationTitleSuffix
		}
		val html = wikipediaToHtml(cleanedEntry.getText())
		val parsedEntry = parseHtml(title, html)
		parsedEntry.templatelinks = extractTemplateLinks(cleanedEntry.getText())
		parsedEntry
	}

	def cleanRedirects(entry: WikipediaEntry): WikipediaEntry = {
		val redirectRegex = "(?i)#(weiterleitung|redirect) ?(: ?)?"
		entry.setText(entry.getText().replaceAll(redirectRegex, "WEITERLEITUNG"))
		entry
	}

	/**
	  * Reads namespaces from resource file and removes used namespaces.
	  *
	  * @return list of unused namespaces
	  */
	def badNamespaces(): List[String] = {
		val namespaces = Source
			.fromURL(getClass().getResource(s"/$namespaceFile"))
			.getLines()
			.toList
		val usedNamespaces = List("Kategorie:")
		namespaces.filterNot(usedNamespaces.toSet)
	}

	/**
	  * Checks whether the entry starts with a namespace.
	  *
	  * @param entry Wikipedia entry to check
	  * @return true if entry starts with a namespace
	  */
	def isMetaPage(entry: WikipediaEntry): Boolean = {
		badNamespaces.exists(entry.title.startsWith)
	}

	/**
	  * Removes links to pages with an unused namespace.
	  *
	  * @param linkList list of links to clean
	  * @return list of links without links to unused namespace pages.
	  */
	def cleanMetapageLinks(linkList: List[Link]): List[Link] = {
		linkList.filterNot(link => badNamespaces.exists(link.page.startsWith))
	}

	def extractCategoryLinks(entry: ParsedWikipediaEntry): ParsedWikipediaEntry = {
		val categoryLinks = entry.textlinks.filter(entry =>
			entry.alias.startsWith(categoryNamespace) || entry.page.startsWith(categoryNamespace))
		entry.textlinks = entry.textlinks.filterNot(categoryLinks.toSet)
		entry.categorylinks = categoryLinks.map { link =>
			val alias = link.alias.replaceFirst(categoryNamespace, "")
			val page = link.page.replaceFirst(categoryNamespace, "")
			Link(alias, page, 0)
		}
		entry
	}

	def extractListLinks(html: String): List[Link] = {
		val document = Jsoup.parse(html)
		val htmlLists = document.select("ul").toList ++ document.select("ol").toList
		htmlLists
			.map(element => Jsoup.parse(element.html()))
			.flatMap(_.select("a"))
			.map(anchor => Link(anchor.text, parseUrl(anchor.attr("href"))))
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Textparser")
		val sc = new SparkContext(conf)

		val wikipedia = sc.cassandraTable[WikipediaEntry](keyspace, tablename)
		wikipedia
			.map(parseWikipediaEntry)
			.saveToCassandra(keyspace, outputTablename)
		sc.stop
	}
}
