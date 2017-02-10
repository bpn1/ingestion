import scala.collection.mutable

object WikiClasses {

	case class Link(alias: String, var page: String, offset: Int)
	case class WikipediaEntry(title: String, var text: Option[String]) {
		def setText(t: String): Unit = {
			text = Option(t)
		}
		def getText(): String = text match {
			case Some(t) => t
			case None => ""
		}
	}
	case class ParsedWikipediaEntry(title: String, var text: Option[String], var links: List[Link], var foundaliases: List[String] = List[String]()) {
		def setText(t: String): Unit = {
			text = Option(t)
		}
		def getText(): String = text match {
			case Some(t) => t
			case None => ""
		}
	}

	case class AliasCounter(alias: String, var linkOccurrences: Int = 0, var totalOccurrences: Int = 0)
	case class AliasOccurrencesInArticle(links: mutable.Set[String], noLinks: mutable.Set[String])

	// Seq is Serializable, Map isn't
	case class Alias(alias: String, pages: Seq[(String, Int)])
	case class Page(page: String, aliases: Seq[(String, Int)])

}
