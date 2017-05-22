package de.hpi.ingestion.textmining.models

/**
  * Case class representing document frequency of a term.
  *
  * @param entitylist  the term
  * @param count the number of documents the term appears in
  */
case class Cooccurrence(entitylist: List[String], count: Int)
