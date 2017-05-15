package de.hpi.ingestion.textmining.models

/**
  *
  * @param alias
  * @param link
  * @param linkContext
  * @param totalProbability
  * @param pageProbability
  * @param pageRank rank of pageProbability compared to pageProbability of other ProtoFeatureEntries with same alias
  */
case class ProtoFeatureEntry(
	alias: String,
	link: Link,
	linkContext: Map[String, Double],
	totalProbability: Double,
	pageProbability: Double,
	pageRank: Int
)
