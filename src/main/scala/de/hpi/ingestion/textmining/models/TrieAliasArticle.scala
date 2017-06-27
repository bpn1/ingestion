package de.hpi.ingestion.textmining.models

/**
  * Represents an article used for NEL.
  * @param article unique identifier of the article
  * @param text content of the article
  * @param triealiases aliases found with the trie
  * @param foundentities entities found with the classifier
  */
case class TrieAliasArticle(
	article: String,
	text: Option[String] = None,
	triealiases: List[TrieAlias] = Nil,
	foundentities: List[Link] = Nil
)
