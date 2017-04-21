package de.hpi.ingestion.textmining.models

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
