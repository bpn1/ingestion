package de.hpi.ingestion.dataimport.dbpedia.models

/**
  * Class representing an entity of the DBPediaDump.
  * @param dbpedianame Name of the entity
  * @param wikipageid Id of the wikipedia page
  * @param label Label of the entity
  * @param description Description of the entity
  * @param instancetype Type of the entity
  * @param data all other information about the entity
  */
case class DBPediaEntity(
	dbpedianame: String,
	var wikipageid: Option[String] = None,
	var label: Option[String] = None,
	var description: Option[String] = None,
	var instancetype: Option[String] = None,
	var data: Map[String, List[String]] = Map[String, List[String]]())
