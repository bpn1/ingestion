package de.hpi.ingestion.textmining.models

import scala.collection.mutable.ListBuffer

/**
  * Case class representing a Parsed Wikipedia article.
  *
  * @param title               title of the page
  * @param text                plain text of the page
  * @param textlinks           all links appearing in the text
  * @param templatelinks       all links appearing in Wikimarkup templates (e.g. infoboxes)
  * @param foundaliases        all aliases (of all links) found in the plain text
  * @param categorylinks       all links of category pages on the page
  * @param listlinks           all links appearing in lists
  * @param disambiguationlinks all links on this page if this page is a disambiguation page
  * @param linkswithcontext    all textlinks containing the term frequencies of their context
  * @param context             term frequencies of this articles plain text
  * @param triealiases         the longest aliases found by the trie with their offset and context
  * @param rawextendedlinks    all occurrences of aliases of other links in this article
  */
case class ParsedWikipediaEntry(
	title: String,
	var text: Option[String] = None,
	var textlinks: List[Link] = Nil,
	var templatelinks: List[Link] = Nil,
	var foundaliases: List[String] = Nil,
	var categorylinks: List[Link] = Nil,
	var listlinks: List[Link] = Nil,
	var disambiguationlinks: List[Link] = Nil,
	var linkswithcontext: List[Link] = Nil,
	var context: Map[String, Int] = Map[String, Int](),
	var triealiases: List[TrieAlias] = Nil,
	var rawextendedlinks: List[ExtendedLink] = Nil
) {
	def setText(t: String): Unit = text = Option(t)

	def getText(): String = text.getOrElse("")

	/**
	  * Concatenates all link lists extracted from the Wikipedia article.
	  *
	  * @return all links
	  */
	def allLinks(): List[Link] = {
		textlinks ++ templatelinks ++ categorylinks ++ listlinks ++ disambiguationlinks ++ extendedlinks()
	}

	/**
	  * Executes a filter function on all link lists.
	  *
	  * @param filterFunction filter function
	  */
	def filterLinks(filterFunction: Link => Boolean): Unit = {
		textlinks = textlinks.filter(filterFunction)
		templatelinks = templatelinks.filter(filterFunction)
		categorylinks = categorylinks.filter(filterFunction)
		disambiguationlinks = disambiguationlinks.filter(filterFunction)
		listlinks = listlinks.filter(filterFunction)
	}

	/**
	  * Filters the linkswithcontext links and returns only the textlinks with their contexts.
	  *
	  * @return textlinks and their contexts
	  */
	def textlinkContexts(): List[Link] = {
		linkswithcontext.filter(contextLink =>
			textlinks.exists(link => link.alias == contextLink.alias && link.offset == contextLink.offset))
	}

	/**
	  * Filters the linkswithcontext links and returns only the extendedlinks with their contexts.
	  *
	  * @return extendedlinks and their contexts
	  */
	def extendedlinkContexts(): List[Link] = {
		linkswithcontext.filter(contextLink =>
			extendedlinks().exists(link => link.alias == contextLink.alias && link.offset == contextLink.offset))
	}

	/**
	  * Filters and parses Extended Links to Links
	  *
	  * @param noTextLinks defines if you do not want extended links that are colliding with text links, default is true
	  * @param countThresh defines the threshold of occurrences of alias it has to pass
	  *                    if there are colliding extended links
	  * @param normalizedThresh defines the threshold of normalized occurrences of alias it has to pass
	  *                         if there are colliding extended links
	  * @return List of (filtered) Links
	  */
	def extendedlinks(noTextLinks: Boolean = true, countThresh: Int = 1, normalizedThresh: Double = 0.1): List[Link] = {
		var filteredLinks = rawextendedlinks
			.map(t => (t, t.filterExtendedLink(countThresh, normalizedThresh)))
			.collect {
				case (t, Some(page)) => (t, page)
			}.map { case (exLink, page) =>
			Link(exLink.alias, page, exLink.offset)
		}
		if(noTextLinks) {
			filteredLinks = filterCollidingLinks(filteredLinks, textlinks)
		}
		filteredLinks
	}

	/**
	  * Filters extended links by overlapping offsets of text links in O(nlogn).
	  *
	  * @param extendedLinks List of extended links that will be filtered
	  * @param textLinks     List of text links that is used to filter
	  * @return filtered List of extended links without colliding text links
	  */
	def filterCollidingLinks(extendedLinks: List[Link], textLinks: List[Link]): List[Link] = {
		val orderedExtendedLinks = extendedLinks.filter(_.offset.exists(_ >= 0)).sortBy(_.offset)
		val orderedTextLinks = textLinks.filter(_.offset.exists(_ >= 0)).sortBy(_.offset)
		var resultList = new ListBuffer[Link]()
		var (i, j) = (0, 0)
		while(j < orderedTextLinks.length && i < orderedExtendedLinks.length) {
			val startEL = orderedExtendedLinks(i).offset.get
			val endEL = startEL + orderedExtendedLinks(i).alias.length - 1
			var startTL = orderedTextLinks(j).offset.get
			var endTL = startTL + orderedTextLinks(j).alias.length - 1
			// find the next text link that might be colliding
			while(endTL < startEL && j < orderedTextLinks.length - 1) {
				j += 1
				startTL = orderedTextLinks(j).offset.get
				endTL = startTL + orderedTextLinks(j).alias.length - 1
			}
			if(!checkIfColliding(startEL, endEL, startTL, endTL)) {
				resultList += orderedExtendedLinks(i)
			}
			i += 1
		}
		// if there are no text links but extended links left
		// add the remaining extended links because they cannot collide with anything
		while(i < orderedExtendedLinks.length) {
			resultList += orderedExtendedLinks(i)
			i += 1
		}
		resultList.toList
	}

	/**
	  * checks if two links are colliding by checking their start and end offsets.
	  *
	  * @param startEL start offset of extended link
	  * @param endEL   end offset of extended link
	  * @param startTL start offset of text link
	  * @param endTL   end offset of text link
	  * @return false if they are colliding if not false
	  */
	def checkIfColliding(startEL: Int, endEL: Int, startTL: Int, endTL: Int): Boolean = {
		// startTL---startEL----endTL or
		(startTL <= startEL && startEL <= endTL) ||
			// startTL---endEL----endTL or
			(startTL <= endEL && endEL <= endTL) ||
			// startEL---startTL---endTL---EndEL
			(startEL <= startTL && endTL <= endEL)
	}
}
