package de.hpi.ingestion.versioncontrol.models

import java.util.UUID

/**
  * Contains the values of an older and a newer version for every field of a Subject.
  * @param id UUID of the Subject
  * @param name tuple of old and new name values
  * @param master tuple of old and new master values
  * @param aliases tuple of old and new alias values
  * @param category tuple of old and new category values
  * @param properties tuple of old and new values for every property
  * @param relations tuple of old and new values for every property of every relation
  */
case class HistoryEntry(
	id: UUID,
	name: Option[(List[String], List[String])] = None,
	master: Option[(List[String], List[String])] = None,
	aliases: Option[(List[String], List[String])] = None,
	category: Option[(List[String], List[String])] = None,
	properties: Map[String, Option[(List[String], List[String])]] = Map(),
	relations: Map[UUID, Map[String, Option[(List[String], List[String])]]] = Map()
)
