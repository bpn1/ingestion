package de.hpi.ingestion.dataimport.wikipedia.models

case class WikipediaEntry(
	title: String,
	var text: Option[String] = None
){
	def setText(t: String): Unit = text = Option(t)
	def getText(): String = text.getOrElse("")
}
