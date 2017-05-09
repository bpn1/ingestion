package de.hpi.ingestion.textmining.models

/**
  * Case class representing a Entity.
  *
  * @param alias   alias this entity appears as in this sentence
  * @param page    page this entity points to
  * @param offset  character offset of this alias in the plain text it appears in.
  */
case class Entity(
	alias: String,
	var page: String,
	var offset: Option[Int] = None
)
