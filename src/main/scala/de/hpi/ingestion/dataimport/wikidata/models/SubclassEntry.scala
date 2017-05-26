package de.hpi.ingestion.dataimport.wikidata.models

/**
  * Represents the subclass data of a Wikidata entity.
  * @param id Wikidata Id of the entity
  * @param label label of the entity
  * @param data filtered data map containing only the subclass of and instance of properties
  * @param classList list of subclasses of this entry
  */
case class SubclassEntry(
	id: String,
	var label: String = "",
	var data: Map[String, List[String]] = Map[String, List[String]](),
	var classList: List[String] = Nil)
