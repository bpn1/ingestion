package de.hpi.ingestion.dataimport.wikipedia

import de.hpi.ingestion.dataimport.wikipedia.models.WikipediaEntry

import scala.io.Source

object TestData {

	def pageXML: List[String] = {
		Source.fromURL(getClass.getResource("/wikipedia/raw_pages.xml")).getLines.toList
	}

	def wikipediaEntries: List[WikipediaEntry] = {
		List(
			WikipediaEntry("Alan Smithee", Option("Text 1")),
			WikipediaEntry("Actinium", Option("Text 2")),
			WikipediaEntry("Ang Lee", Option("Text 3")),
			WikipediaEntry("Anschluss (Soziologie)", Option("Text 4")),
			WikipediaEntry("Anschlussfähigkeit", Option("Text 5")))
	}
}
