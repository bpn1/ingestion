package de.hpi.ingestion.dataimport.wikidata.models

case class WikiDataEntity(
	id: String,
	var aliases: List[String] = List[String](),
	var description: Option[String] = None,
	var entitytype: Option[String] = None,
	var wikiname: Option[String] = None,
	var enwikiname: Option[String] = None,
	var instancetype: Option[String] = None,
	var label: Option[String] = None,
	var data: Map[String, List[String]] = Map[String, List[String]]())
