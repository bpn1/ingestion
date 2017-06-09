package de.hpi.ingestion.textmining.models

/**
  * Case class representing document frequency of a term.
  *
  * @param word  the term
  * @param count the number of documents the term appears in
  */
case class DocumentFrequency(word: String, count: Int)
