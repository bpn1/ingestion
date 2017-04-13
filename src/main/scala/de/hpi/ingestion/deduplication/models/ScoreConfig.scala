package de.hpi.ingestion.deduplication.models

import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure

/**
  * Reproduces a configuration for a comparison of two Subjects
  * @param key the attribute of a Subject to be compared
  * @param similarityMeasure the similarity measure used for the comparison
  * @param weight the weight of the result
  * @param scale specifies the n-gram if the similarity measure has one
  * @tparam A type of the attribute
  * @tparam B type of the similarity measure
  */
case class ScoreConfig[A, B <: SimilarityMeasure[A]](
	key: String,
	similarityMeasure: B,
	weight: Double,
	scale: Int = 1)
