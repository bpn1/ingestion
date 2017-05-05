package de.hpi.ingestion.textmining.models

/**
  * Case class representing a token with its respective character offset.
  *
  * @param token  the token itself
  * @param offset character offset within the original text
  */
case class OffsetToken(
	token: String,
	offset: Int
)
