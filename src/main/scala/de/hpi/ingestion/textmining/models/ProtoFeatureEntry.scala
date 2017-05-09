package de.hpi.ingestion.textmining.models

/**
  *
  * @param alias
  * @param link
  * @param linkContext
  * @param totalProbability
  * @param pageProbability
  */
case class ProtoFeatureEntry(
	alias: String,
	link: Link,
	linkContext: Map[String, Double],
	totalProbability: Double,
	pageProbability: Double
)
