package de.hpi.ingestion.dataimport.spiegel.models

case class SpiegelArticle(
	id: String,
	text: Option[String] = None
)
