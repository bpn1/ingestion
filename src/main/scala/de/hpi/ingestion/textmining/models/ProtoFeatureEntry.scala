package de.hpi.ingestion.textmining.models

/**
  * Prototype for a Feature entry.
  * @param alias alias of the entry
  * @param link link this entry represents
  * @param linkContext context of the link containing the tfidf values of the terms
  * @param totalProbability probability this alias is a link
  * @param pageProbability probability this alias points to the specific page this object will be keyed by
  */
case class ProtoFeatureEntry(
	alias: String,
	link: Link,
	linkContext: Map[String, Double],
	totalProbability: Double,
	pageProbability: Double
)
