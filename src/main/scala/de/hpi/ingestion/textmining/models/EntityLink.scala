package de.hpi.ingestion.textmining.models

/**
  * Case class representing a link to an entity.
  *
  * @param alias   alias this entity appears as in this sentence
  * @param entity  entity this entity points to
  * @param offset  character offset of this alias in the plain text it appears in.
  */
case class EntityLink(alias: String, entity: String, offset: Option[Int] = None)
