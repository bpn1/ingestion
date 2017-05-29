package de.hpi.ingestion.textmining.models

/**
  * Represents an alias found with the trie and contains the alias, its index and its context.
  * @param alias alias found in the text
  * @param offset character offset of the alias in the text
  * @param context term frequencies of the aliases context
  */
case class TrieAlias(
	alias: String,
	offset: Option[Int] = None,
	context: Map[String, Int] = Map())
