package de.hpi.ingestion.textmining.models

// TODO scaladocs
case class RelationPrediction(
	subjectentity: String,
	relationtype: String,
	objectentity: String,
	dbpedia: Boolean,
	count: Int
)