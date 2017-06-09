
package de.hpi.ingestion.deduplication.models.config

import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure

/**
  * Reproduces a configuration for a comparison of two Subjects
  * @param similarityMeasure the similarity measure used for the comparison
  * @param weight the weight of the result
  * @param scale specifies the n-gram if the similarity measure has one
  * @tparam A type of the attribute
  * @tparam B type of the similarity measure
  */
case class SimilarityMeasureConfig[A, B <: SimilarityMeasure[A]](
	similarityMeasure: B,
	weight: Double = 0.0,
	scale: Int = 1
) extends WeightedFeatureConfig{
	override type T = SimilarityMeasureConfig[A, B]
	override def updateWeight(weight: Double): SimilarityMeasureConfig[A, B] = this.copy(weight = weight)

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
