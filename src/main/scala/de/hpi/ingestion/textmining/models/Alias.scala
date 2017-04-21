package de.hpi.ingestion.textmining.models

case class Alias(
	alias: String,
	pages: Map[String, Int] = Map(),
	var linkoccurrences: Int = 0,
	var totaloccurrences: Int = 0
)
