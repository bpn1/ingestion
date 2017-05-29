package de.hpi.ingestion.deduplication.models

import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure

/**
  * Reproduces a configuration for a comparison of two Subjects
  * @param similarityMeasure the similarity measure used for the comparison
  * @param weight the weight of the result
  * @param scale specifies the n-gram if the similarity measure has one
  * @tparam A type of the attribute
  * @tparam B type of the similarity measure
  */
case class ScoreConfig[A, B <: SimilarityMeasure[A]](
	similarityMeasure: B,
	weight: Double,
	scale: Int = 1
) {
	/**
	  * This method simply compares two strings.
	  *
	  * @param leftValue  String to be compared with rightValue.
	  * @param rightValue String to be compared with leftValue.
	  * @return The similarity score of the two input strings.
	  */
	def compare(leftValue: A, rightValue: A): Double = {
		val score = similarityMeasure.compare(leftValue, rightValue, scale)
		score * weight
	}
}
