package de.hpi.ingestion.textmining.models

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
  * @param textlinksreduced    all links appearing in the text with aliases of companies
  * @param templatelinksreduced all links appearing in Wikimarkup templates (e.g. infoboxes) with aliases of companies
  * @param categorylinksreduced all links of category pages on the page with aliases of companies
  * @param listlinksreduced    all links appearing in lists with aliases of companies
  * @param disambiguationlinksreduced all links on this page if this page is a disambiguation page with aliases of
  *                                   companies
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
	var rawextendedlinks: List[ExtendedLink] = Nil,
	var textlinksreduced: List[Link] = Nil,
	var templatelinksreduced: List[Link] = Nil,
	var categorylinksreduced: List[Link] = Nil,
	var listlinksreduced: List[Link] = Nil,
	var disambiguationlinksreduced: List[Link] = Nil
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
	  * Concatenates all reduced link lists extracted from the Wikipedia article.
	  * @return all reduced links
	  */
	def reducedLinks(): List[Link] = {
		List(
			textlinksreduced,
			templatelinksreduced,
			categorylinksreduced,
			listlinksreduced,
			disambiguationlinksreduced,
			extendedlinks()).flatten
	}

	/**
	  * Executes a filter function on every not reduced link list.
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
	  * Executes a filter function on every not reduced link list and saves the result to the reduced columns.
	  * @param filterFunction filter function
	  */
	def filterLinksToReduced(filterFunction: Link => Boolean): Unit = {
		textlinksreduced = textlinks.filter(filterFunction)
		templatelinksreduced = templatelinks.filter(filterFunction)
		categorylinksreduced = categorylinks.filter(filterFunction)
		disambiguationlinksreduced = disambiguationlinks.filter(filterFunction)
		listlinksreduced = listlinks.filter(filterFunction)
	}

	/**
	  * Filters and parsed Extended Links to Links
	  * @return List of parsed Links
	  */
	def extendedlinks(): List[Link] = {
		rawextendedlinks
			.map(t => (t, t.filterExtendedLink(1, 0.1)))
			.collect {
				case (t, Some(page)) => (t, page)
			}.map { case (exLink, page) =>
				Link(exLink.alias, page, exLink.offset)
			}
	}

}
