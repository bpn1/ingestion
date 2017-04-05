package de.hpi.ingestion.dataimport.wikidata.models

case class SubclassEntry(
	id: String,
	var label: String = "",
	var data: Map[String, List[String]] = Map[String, List[String]](),
	var classList: List[String] = List[String]())
