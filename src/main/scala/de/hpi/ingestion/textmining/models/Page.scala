package de.hpi.ingestion.textmining.models

/**
  * Case class representing a Wikipedia page and all aliases that link to this page.
  *
  * @param page    name of this page
  * @param aliases all aliases that link to this page and how often it does with the respective rank
  */
case class Page(
	page: String,
	aliases: Map[String, RankedValue] = Map()
)
