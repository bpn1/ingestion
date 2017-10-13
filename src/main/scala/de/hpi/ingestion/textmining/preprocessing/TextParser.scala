/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.textmining.preprocessing

import java.net.URLDecoder

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.wikipedia.models.WikipediaEntry
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models._
import info.bliki.wiki.model.WikiModel
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.jsoup.Jsoup
import org.jsoup.nodes.{Comment, Document, Element, TextNode}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.matching.Regex

/**
  * Parses all Wikipedia articles from wikitext to raw text and extracts its links.
  */
class TextParser extends SparkJob {
	import TextParser._
	appName = "Text Parser"
	configFile = "textmining.xml"

	var wikipedia: RDD[WikipediaEntry] = _
	var parsedWikipedia: RDD[ParsedWikipediaEntry] = _

	// $COVERAGE-OFF$
	/**
	  * Loads Wikipedia entries from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		wikipedia = sc.cassandraTable[WikipediaEntry](settings("keyspace"), settings("rawWikiTable"))
	}

	/**
	  * Saves Parsed Wikipedia entries to the Cassandra.
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		parsedWikipedia.saveToCassandra(settings("keyspace"), settings("parsedWikiTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Parses all Wikipedia articles from wikitext to raw text and extracts its links. Also removes all metapages
	  * except categoryNamespace pages.
	  * @param sc Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		parsedWikipedia = wikipedia
			.filter(!isMetaPage(_))
			.map(parseWikipediaEntry)
	}
}

object TextParser {
	val namespaceFile = "dewiki_namespaces"
	val categoryNamespace = "Kategorie:"
	val disambiguationTitleSuffix = " (Begriffsklärung)"
	val redirectText = "REDIRECT"
	val parsableRedirect = "WEITERLEITUNG"

	/**
	  * Checks if article is redirect page.
	  *
	  * @param entry Wikipedia entry to be checked
	  * @return true if article is redirect page
	  */
	def isRedirectPage(entry: ParsedWikipediaEntry): Boolean = {
		entry.getText().startsWith(redirectText)
	}

	/**
	  * Checks if article is redirect page.
	  *
	  * @param html html of article to be checked
	  * @return true if article is redirect page
	  */
	def isRedirectPage(html: String): Boolean = {
		val searchText = Jsoup.parse(html).body.text
		searchText.startsWith(parsableRedirect)
	}

	/**
	  * Cleans wikitext and converts it to html.
	  *
	  * @param wikitext wikitext to clean and convert
	  * @return cleaned html
	  */
	def wikipediaToHtml(wikitext: String): String = {
		val html = wikitext.replaceAll("\\\\n", "\n")
		WikiModel.toHtml(removeWikitextMarkup(html))
	}

	/**
	  * Extracts all links from a redirect page including redirect link.
	  *
	  * @param title title of Wikipedia article
	  * @param html  html of Wikipedia article
	  * @param text  text of Wikipedia article
	  * @return list of all links in the redirect page
	  */
	def extractRedirect(title: String, html: String, text: String): List[Link] = {
		val anchorList = Jsoup.parse(html).select("a")
		val linksList = anchorList
			.map { anchor =>
				val target = parseUrl(anchor.attr("href"))
				val source = if(anchor.text.isEmpty) target else anchor.text
				val offset = Option(text.indexOf(source)).filter(_ >= 0)
				val redirectLink = Link(title, target, offset)
				val defaultLink = Link(source, target)
				if(isCategoryLink(defaultLink)) defaultLink else redirectLink
			}
			.toList
		cleanMetapageLinks(linksList)
	}

	/**
	  * Parse Wikipedia article, remove the wikitext markup, extract text and all links.
	  *
	  * @param title title of Wikipedia article
	  * @param html  html of Wikipedia article
	  * @return parsed Wikipedia entry
	  */
	def parseHtml(title: String, html: String): ParsedWikipediaEntry = {
		val parsedEntry = ParsedWikipediaEntry(title)
		val document = Jsoup.parse(html)
		if(isRedirectPage(html)) {
			val text = document.body.text.replaceAll("(?i)(\\A(WEITERLEITUNG|REDIRECT))", redirectText)
			parsedEntry.setText(text)
			parsedEntry.textlinks = extractRedirect(title, html, text)
		} else if(title.endsWith(disambiguationTitleSuffix)) {
			val outputTuple = extractDisambiguationLinks(title, document.body)
			parsedEntry.setText(outputTuple._1)
			parsedEntry.disambiguationlinks = outputTuple._2
		} else {
			val cleanedDocument = removeTags(document)
			val outputTuple = extractTextAndTextLinks(cleanedDocument.body)
			parsedEntry.setText(outputTuple._1)
			parsedEntry.textlinks = outputTuple._2
			parsedEntry.listlinks = extractListLinks(html)
		}
		extractCategoryLinks(parsedEntry)
	}

	/**
	  * Removes any unwanted HTML tags and inline wikitext tags.
	  *
	  * @param document Jsoup Document to clean
	  * @return cleaned Document containing only text and anchor tags
	  */
	def removeTags(document: Document): Document = {
		val tableOfContents = document.getElementById("toc")
		if(tableOfContents != null) {
			tableOfContents.remove()
		}

		// https://www.w3schools.com/html/html_headings.asp
		val documentContent = document
			.select("p,h1,h2,h3,h4,h5,h6")
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
		val doc = Jsoup.parse(htmltext)
		val traverser = new WikipediaNodeVisitor()
		doc.traverse(traverser)
		Jsoup.parse(traverser.getCleanedDocument())
	}

	/**
	  * Parse url to match Wikipedia titles.
	  *
	  * @param url url found in anchor tags
	  * @return title of linked Wikipedia page
	  */
	def parseUrl(url: String): String = {
		var cleanedUrl = url
			.replaceAll("%(?![0-9a-fA-F]{2})", "%25")
			.replaceAll("\\+", "%2B")
		try {
			cleanedUrl = URLDecoder.decode(cleanedUrl, "UTF-8")
				.replaceAll("_", " ")
		} catch {
			case e: java.lang.IllegalArgumentException => cleanedUrl = url
		}
		cleanedUrl.replaceAll("\\A/", "")
	}

	/**
	  * Checks if article is disambiguation page.
	  *
	  * @param entry Wikipedia entry to be checked
	  * @return true if article is disambiguation page
	  */
	def isDisambiguationPage(entry: WikipediaEntry): Boolean = {
		val disambiguationWikitextSuffix = "{{Begriffsklärung}}"
		entry.getText().endsWith(disambiguationWikitextSuffix)
	}

	/**
	  * Extracts all links from a disambiguation page.
	  *
	  * @param title title of Wikipedia article
	  * @param body  html body of Wikipedia article
	  * @return tuple of html text and list of all links
	  */
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

	/**
	  * Extract text and all links with offset from html.
	  *
	  * @param body html body of Wikipedia article
	  * @return tuple of html text and list of all text links
	  */
	def extractTextAndTextLinks(body: Element): (String, List[Link]) = {
		val linksList = ListBuffer[Link]()
		var offset = 0
		var startIndex = 0
		val children = body.childNodes

		if(children.isEmpty) {
			return ("", linksList.toList)
		}

		if(children.head.isInstanceOf[TextNode]) {
			startIndex = 1
			var firstChildText = children(0).asInstanceOf[TextNode].text
			while(firstChildText.charAt(0) != body.text.headOption.getOrElse("")) {
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
					val link = Link(source, target, Option(offset))
					linksList += link
					offset += t.text.length
				case t: TextNode =>
					offset += t.text.length
				case t: Comment =>
				// ignore HTML comments
			}
		}
		val cleanedLinks = cleanMetapageLinks(linksList.toList)
		(body.text, cleanedLinks)
	}

	/**
	  * Removes text of wikitext.
	  *
	  * @param wikitext wikitext text to be cleaned
	  * @return cleaned text
	  */
	def removeWikitextMarkup(wikitext: String): String = {
		var cleanText = ""
		var depth = 0
		var escaped = false
		for(character <- wikitext) {
			if(!escaped && character == '{') {
				depth += 1
			} else if(!escaped && character == '}' && depth > 0) {
				depth -= 1
			} else if(depth == 0) {
				cleanText += character
			}
			escaped = character == '\\'
		}
		cleanText
	}

	/**
	  * Extracts and checks if Wikipedia template is correct.
	  *
	  * @param wikitext   wikitext text to be extracted from
	  * @param startIndex startIndex of template
	  * @return full template text
	  */
	def extractTemplate(wikitext: String, startIndex: Int): String = {
		var templateText = ""
		var depth = 0
		var escaped = false
		for(i <- startIndex until wikitext.length) {
			val character = wikitext(i)
			if(!escaped && character == '{') {
				depth += 1
			} else if(!escaped && character == '}' && depth > 0) {
				depth -= 1
			}
			templateText += character
			if(depth == 0) {
				return templateText
			}
			escaped = character == '\\'
		}
		templateText
	}

	/**
	  * Extract all templates in Wikipedia article.
	  *
	  * @param wikitext wikitext text to be extracted from
	  * @return template text containing all the templates concatenated
	  */
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

		for(startIndex <- startIndices) {
			templateText += extractTemplate(wikitext, startIndex)
		}
		templateText
	}

	/**
	  * Constructs a link from regex match.
	  *
	  * @param linkMatch regex match containing link
	  * @return link constructed from regex
	  */
	def constructLinkFromRegexMatch(linkMatch: Regex.Match): Option[Link] = {
		Option(linkMatch.group(1))
			.filter(!_.startsWith("Datei:"))
			.map { page =>
				val alias = Option(linkMatch.group(2))
					.map(_.stripPrefix("|"))
					.filter(_.nonEmpty)
					.getOrElse(page)
				Link(alias, page)
			}
	}

	/**
	  * Extract all links from Wikipedia template.
	  *
	  * @param wikitext wikitext text to be extracted from
	  * @return list of all links in the template
	  */
	def extractTemplateLinks(wikitext: String): List[Link] = {
		val linkList = ListBuffer[Link]()
		val templateText = extractAllTemplates(wikitext)
		val linkRegex = new Regex("\\[\\[(.*?)(\\|(.*?))?\\]\\]")
		linkRegex
			.findAllIn(templateText)
			.matchData
			.foreach { linkMatch =>
				val link = constructLinkFromRegexMatch(linkMatch)
				link.foreach(link => linkList += link)
			}
		linkList.toList
	}

	/**
	  * Parses a Wikipedia entry meanwhile removing wikitext and extracting all links.
	  *
	  * @param entry raw Wikipedia entry
	  * @return parsed Wikipedia entry thats been processed
	  */
	def parseWikipediaEntry(entry: WikipediaEntry): ParsedWikipediaEntry = {
		val cleanedEntry = cleanRedirectsAndWhitespaces(entry)
		var title = cleanedEntry.title
		if(isDisambiguationPage(cleanedEntry) && !title.endsWith(disambiguationTitleSuffix)) {
			title += disambiguationTitleSuffix
		}
		val html = wikipediaToHtml(cleanedEntry.getText())
		val parsedEntry = parseHtml(title, html)
		parsedEntry.templatelinks = extractTemplateLinks(cleanedEntry.getText())
		parsedEntry
	}

	/**
	  * Cleans all kinds of redirects so they will get parsed by the Wikiparser
	  * and replaces alternative whitespace characters with standard whitespaces.
	  *
	  * @param entry raw Wikipedia entry
	  * @return raw Wikipedia entry with cleaned redirect keywords
	  */
	def cleanRedirectsAndWhitespaces(entry: WikipediaEntry): WikipediaEntry = {
		val redirectRegex = "(?i)(weiterleitung|redirect)\\s?:?\\s?"
		val replaceString = parsableRedirect + " "
		var cleanedText = entry.getText().replaceAll(redirectRegex, replaceString)
		cleanedText = cleanedText.replaceAll("(\u00a0|&nbsp;)", " ")
		entry.setText(cleanedText)
		entry
	}

	/**
	  * Reads namespaces from resource file and removes used namespaces.
	  *
	  * @return list of unused namespaces
	  */
	def badNamespaces(): List[String] = {
		val namespaces = Source
			.fromURL(getClass.getResource(s"/$namespaceFile"))
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
		badNamespaces().exists(entry.title.startsWith)
	}

	/**
	  * Removes links to pages with an unused namespace.
	  *
	  * @param linkList list of links to clean
	  * @return list of links without links to unused namespace pages.
	  */
	def cleanMetapageLinks(linkList: List[Link]): List[Link] = {
		linkList.filterNot(link => badNamespaces().exists(link.page.startsWith))
	}

	/**
	  * Checks if a link is a category link.
	  *
	  * @param link link to be checked
	  * @return true if the link is a category link
	  */
	def isCategoryLink(link: Link): Boolean = {
		link.alias.startsWith(categoryNamespace) || link.page.startsWith(categoryNamespace)
	}

	/**
	  * Extracts all category links from the textlinks column.
	  *
	  * @param entry without category links
	  * @return entry with category links
	  */
	def extractCategoryLinks(entry: ParsedWikipediaEntry): ParsedWikipediaEntry = {
		val categoryLinks = entry.textlinks.filter(isCategoryLink)
		entry.textlinks = entry.textlinks.filterNot(categoryLinks.toSet)
		entry.categorylinks = categoryLinks.map { link =>
			val alias = link.alias.replaceFirst(categoryNamespace, "")
			val page = link.page.replaceFirst(categoryNamespace, "")
			Link(alias, page)
		}
		entry
	}

	/**
	  * Extracts all links in list items from html.
	  *
	  * @param html html text to be extracted from
	  * @return list of all links extracted from list items
	  */
	def extractListLinks(html: String): List[Link] = {
		val document = Jsoup.parse(html)
		val htmlLists = document.select("ul").toList ++ document.select("ol").toList
		htmlLists
			.map(element => Jsoup.parse(element.html()))
			.flatMap(_.select("a"))
			.map(anchor => Link(anchor.text, parseUrl(anchor.attr("href"))))
	}
}
