package de.hpi.ingestion.textmining.models

/**
  * Case class representing an article preprocessed for NEL, e.g, a newspaper or Wikipedia article.
  *
  * @param id		     identifier of the article within its data source, e.g., a title or an ID
  * @param title	     title of the article
  * @param text          raw content of the article
  * @param triealiases   the longest aliases found by the trie with their offset and context
  * @param foundentities entities found by the classifier
  */
case class TrieAliasArticle(
	id: String,
	title: Option[String] = None,
	text: Option[String] = None,
	triealiases: List[TrieAlias] = Nil,
	foundentities: List[Link] = Nil
) {
	def getText: String = text.getOrElse("")
}
