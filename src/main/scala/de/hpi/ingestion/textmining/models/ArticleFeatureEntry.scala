package de.hpi.ingestion.textmining.models

/**
  * Contains all feature entries extracted from a single article.
  * @param article article the feature entries were extracted from
  * @param featureentries the extracted feature entries
  */
case class ArticleFeatureEntry(
	article: String,
	featureentries: List[FeatureEntry] = Nil
)
