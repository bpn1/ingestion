package de.hpi.ingestion.textmining.models

case class ParsedWikipediaEntry(
	title: String,
	var text: Option[String] = None,
	var textlinks: List[Link] = List[Link](),
	var templatelinks: List[Link] = List[Link](),
	var foundaliases: List[String] = List[String](),
	var categorylinks: List[Link] = List[Link](),
	var disambiguationlinks: List[Link] = List[Link](),
	var listlinks: List[Link] = List[Link]()
){
	def setText(t: String): Unit = text = Option(t)
	def getText(): String = text.getOrElse("")
	def allLinks(): List[Link] = {
		textlinks ++ templatelinks ++ categorylinks ++ listlinks ++ disambiguationlinks
	}
}
