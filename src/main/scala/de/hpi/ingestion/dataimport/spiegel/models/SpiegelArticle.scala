package de.hpi.ingestion.dataimport.spiegel.models

import de.hpi.ingestion.textmining.models.TrieAlias

/**
  * Represents a Spiegel Article.
  * @param id id of the article (used in the dump)
  * @param title title of the article
  * @param text raw text content of the article
  * @param url url of the article
  * @param triealiases all aliases found with the trie
  */
case class SpiegelArticle(
	id: String,
	title: Option[String] = None,
	text: Option[String] = None,
	url: Option[String] = None,
	triealiases: List[TrieAlias] = Nil
)
