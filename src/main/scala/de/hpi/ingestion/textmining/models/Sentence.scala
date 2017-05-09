package de.hpi.ingestion.textmining.models

/**
  * Case class representing a Wikipedia page and all aliases that link to this page.
  *
  * @param text    name of this page
  * @param links all aliases that link to this page and how often they do
  */
case class Sentence(text: String, links: List[Link])
