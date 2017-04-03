object WikiClasses {

	case class Link(alias: String, var page: String, var offset: Int = -1)

	case class WikipediaEntry(
		title: String,
		var text: Option[String] = None)
	{
		def setText(t: String): Unit = text = Option(t)

		def getText(): String = text.getOrElse("")
	}

	case class ParsedWikipediaEntry(
		title: String,
		var text: Option[String] = None,
		var textlinks: List[Link] = List[Link](),
		var templatelinks: List[Link] = List[Link](),
		var foundaliases: List[String] = List[String](),
		var categorylinks: List[Link] = List[Link](),
		var disambiguationlinks: List[Link] = List[Link](),
		var listlinks: List[Link] = List[Link]())
	{
		def setText(t: String): Unit = text = Option(t)

		def getText(): String = text.getOrElse("")
		def allLinks(): List[Link] = {
			textlinks ++ templatelinks ++ categorylinks ++ listlinks ++ disambiguationlinks
		}
	}

	case class AliasCounter(
		alias: String,
		var linkoccurrences: Int = 0,
		var totaloccurrences: Int = 0)

	case class AliasOccurrencesInArticle(links: Set[String], noLinks: Set[String])

	case class LinkContext(pagename: String, words: Set[String])

	case class DocumentFrequency(word: String, count: Int)

	case class Alias(alias: String, pages: Map[String, Int])

	case class Page(page: String, aliases: Map[String, Int])

}
