package de.hpi.ingestion.textmining.models

case class AliasCounts(
	alias: String,
	var linkoccurrences: Int = 0,
	var totaloccurrences: Int = 0)
