package de.hpi.ingestion.textmining.models

// TODO
case class Sentence(
	articletitle: String,
	articleoffset: Int,
	text: String,
	entities: List[EntityLink],
	bagofwords: List[String]
)
