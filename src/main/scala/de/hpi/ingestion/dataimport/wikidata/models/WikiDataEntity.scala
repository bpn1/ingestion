package de.hpi.ingestion.dataimport.wikidata.models

case class WikiDataEntity(
	var id: String = "",
	var entitytype: Option[String] = None,
	var instancetype: Option[String] = None,
	var wikiname: Option[String] = None,
	var description: Option[String] = None,
	var label: Option[String] = None,
	var aliases: List[String] = List[String](),
	var data: Map[String, List[String]] = Map[String, List[String]]()
)
