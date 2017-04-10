package de.hpi.ingestion.textmining.models

case class TfidfContext(page: String, tokens: Map[String, Double])
