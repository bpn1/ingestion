package de.hpi.ingestion.textmining.models

/**
  * Case class representing a link between Wikipedia pages.
  *
  * @param alias   text this link appears as on the page
  * @param page    page this link points to
  * @param offset  character offset of this alias in the plain text of the page it appears in
  * @param context term frequencies of the context of this link
  * @param article article from which the link was retrieved
  */
case class Link(
	alias: String,
	var page: String,
	var offset: Option[Int] = None,
	var context: Map[String, Int] = Map[String, Int](),
	var article: Option[String] = None
)
