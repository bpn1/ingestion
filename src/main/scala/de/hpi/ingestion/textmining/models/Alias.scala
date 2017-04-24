package de.hpi.ingestion.textmining.models

case class Alias(
	alias: String,
	pages: Map[String, Int] = Map(),
	var linkoccurrences: Option[Int] = None,
	var totaloccurrences: Option[Int] = None
)
