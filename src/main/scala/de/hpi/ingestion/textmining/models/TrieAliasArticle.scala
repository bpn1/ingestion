package de.hpi.ingestion.textmining.models

/**
  * #todo
  */
case class TrieAliasArticle(
	article: String,
	text: Option[String],
	triealiases: List[TrieAlias] = Nil
)
