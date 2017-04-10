package de.hpi.ingestion.textmining.models

case class PageTermFrequencies(page: String, tokens: Bag[String, Double])
