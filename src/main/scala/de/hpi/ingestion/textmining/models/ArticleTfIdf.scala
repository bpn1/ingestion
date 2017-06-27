package de.hpi.ingestion.textmining.models

case class ArticleTfIdf(
	article: String,
	tfidf: Map[String, Double] = Map()
)
