package de.hpi.ingestion.textmining.models

/**
  * Case class representing Cooccurrences of entities
  *
  * @param entitylist list of entities that appear in one sentence
  * @param count      the number of sentences the entities appear in
  */
case class Cooccurrence(entitylist: List[String], count: Int)
