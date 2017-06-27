package de.hpi.ingestion.textmining.models

/**
  * Case class representing an article preprocessed for NEL, e.g, a newspaper or Wikipedia article.
  *
  * @param article       identifier of the article within its data source, e.g., a title or an ID
  * @param text          raw content of the article
  * @param triealiases   the longest aliases found by the trie with their offset and context
  * @param foundentities entities found by the classifier
  */
case class TrieAliasArticle(
	article: String,
	text: Option[String] = None,
	triealiases: List[TrieAlias] = Nil,
	foundentities: List[Link] = Nil
) {
	def getText: String = text.getOrElse("")
}
