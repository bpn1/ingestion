object WikiClasses {

	case class Link(alias: String, var page: String, offset: Int)

	case class WikipediaEntry(
		title: String,
		var text: Option[String])
	{
		def setText(t: String): Unit = text = Option(t)
		def getText(): String = text.getOrElse("")
	}

	case class ParsedWikipediaEntry(
		title: String,
		var text: Option[String],
		var links: List[Link],
		var foundaliases: List[String] = List[String]())
	{
		def setText(t: String): Unit = text = Option(t)
		def getText(): String = text.getOrElse("")
	}

	case class AliasCounter(
		alias: String,
		var linkoccurrences: Int = 0,
		var totaloccurrences: Int = 0)

	case class AliasOccurrencesInArticle(links: Set[String], noLinks: Set[String])

	case class LinkContext(pagename: String, words: Set[String])
	case class DocumentFrequency(word: String, count: Int)

	// Seq is Serializable, Map isn't
	case class Alias(alias: String, pages: Seq[(String, Int)])
	case class Page(page: String, aliases: Seq[(String, Int)])

}
