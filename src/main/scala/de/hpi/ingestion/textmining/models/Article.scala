package de.hpi.ingestion.textmining.models

/**
  * Case class representing an article, e.g, a newspaper or Wikipedia article.
  *
  * @param title title of the article
  * @param text  raw content of the article
  */
case class Article(title: String, text: String)
