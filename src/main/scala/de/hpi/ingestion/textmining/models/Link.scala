package de.hpi.ingestion.textmining.models

case class Link(
	alias: String,
	var page: String,
	var offset: Option[Int] = None,
	var context: Map[String, Int] = Map[String, Int]()
)
