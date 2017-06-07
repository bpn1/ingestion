package de.hpi.ingestion.textmining.models

/**
  * Case class representing a Wikipedia page and all aliases that link to this page.
  *
  * @param page    name of this page
  * @param aliases all aliases that link to this page and how often they do
  * @param aliasesreduced all aliases of companies that link to this page and how often they do
  */
case class Page(
	page: String,
	aliases: Map[String, Int] = Map(),
	aliasesreduced: Map[String, Int] = Map()
)
