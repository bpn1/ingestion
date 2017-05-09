package de.hpi.ingestion.textmining.models

/**
  * Case class representing a token with its respective character offset.
  *
  * @param token       the token itself
  * @param beginOffset first character index (inclusive) within the original text
  * @param endOffset   last character index (exclusive) within the original text
  *                    (not redundant since special characters may be encoded alternatively, e.g., "(" becomes "-LRB-")
  */
case class OffsetToken(
	token: String,
	beginOffset: Int,
	endOffset: Int
)
