package de.hpi.ingestion.textmining.models

case class TfidfContext(page: String, tokens: Bag[String, Double])