package de.hpi.ingestion.textmining.models

case class TermFrequencies(page: String, tokens: Bag[String, Int])