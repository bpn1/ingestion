package de.hpi.ingestion.textmining.models

/**
  * An Alias appearing in Wikipedia containing the data about the pages it points to and how often it appears.
  *
  * @param alias            the alias as it appears in the readable text
  * @param pages            all Wikipedia pages this alias points to and how often it does
  * @param linkoccurrences  in how many articles this alias appears as link
  * @param totaloccurrences in how many articles this alias appears in the plain text
  */
case class Alias(
	alias: String,
	pages: Map[String, Int] = Map(),
	var linkoccurrences: Option[Int] = None,
	var totaloccurrences: Option[Int] = None
)
