package de.hpi.ingestion.textmining.models

/**
  * Case class representing a Wikipedia page and all aliases that link to this page.
  *
  * @param text    name of this page
  * @param entities all aliases that link to this page and how often they do
  */
case class Sentence(articletitle: String, articleoffset: Int, text: String, entities: List[Entity])
