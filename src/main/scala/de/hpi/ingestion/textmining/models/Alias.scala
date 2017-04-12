package de.hpi.ingestion.textmining.models

case class Alias(
	alias: String,
	pages: Map[String, Int] = Map(),
	var link_occurrences: Int = 0,
	var total_occurrences: Int = 0
)
