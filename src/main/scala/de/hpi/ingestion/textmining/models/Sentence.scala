package de.hpi.ingestion.textmining.models

/**
  * Case class representing a sentence in an article
  *
  * @param articletitle  title of the article the sentence was extracted from
  * @param articleoffset offset in the article the sentence was extracted from
  * @param text          text of the sentence
  * @param entities      found entity links in this article
  * @param bagofwords    bag of words of the sentence except for the aliases of the entities
  */
case class Sentence(
	articletitle: String,
	articleoffset: Int,
	text: String,
	entities: List[EntityLink],
	bagofwords: List[String]
)
