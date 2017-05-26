package de.hpi.ingestion.textmining.models

/**
  * Stores the aliases that occurred in an article as link or not as link.
  *
  * @param links   Aliases that occurred in an article as link
  * @param noLinks Aliases that occurred in an article, but not as link
  */
case class AliasOccurrencesInArticle(links: Set[String], noLinks: Set[String])
