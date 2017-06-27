package de.hpi.ingestion.textmining.models

/**
  * Represents an entry containing the number of documents in a table.
  * @param countedtable table whose count this is
  * @param count the number of documents in the table
  */
case class WikipediaArticleCount(
	countedtable: String,
	count: BigInt
)
