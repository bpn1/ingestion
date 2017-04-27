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
  */
case class ParsedWikipediaEntry(
	title: String,
	var text: Option[String] = None,
	var textlinks: List[Link] = List[Link](),
	var templatelinks: List[Link] = List[Link](),
	var foundaliases: List[String] = List[String](),
	var categorylinks: List[Link] = List[Link](),
	var listlinks: List[Link] = List[Link](),
	var disambiguationlinks: List[Link] = List[Link](),
	var linkswithcontext: List[Link] = List[Link](),
	var context: Map[String, Int] = Map[String, Int]()
) {
	def setText(t: String): Unit = text = Option(t)

	def getText(): String = text.getOrElse("")

	/**
	  * Concatenates all link lists extracted from the Wikipedia article.
	  *
	  * @return all links
	  */
	def allLinks(): List[Link] = {
		textlinks ++ templatelinks ++ categorylinks ++ listlinks ++ disambiguationlinks
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
}
