package de.hpi.ingestion.textmining.models

/**
  * Case class representing value for a feature with its respective values for the second order features.
  *
  * @param value           original value for the feature
  * @param rank            rank of the original value (1 for the highest value)
  * @param delta_top       absolute difference to the highest value (Double.PositiveInfinity for the highest value)
  * @param delta_successor absolute difference to the successive (next smaller) value
  *                        (Double.PositiveInfinity for the smallest value)
  */
case class MultiFeature(
	value: Double,
	var rank: Int = 1,
	var delta_top: Double = Double.PositiveInfinity,		// NaN and negative values cannot be used to train the
	var delta_successor: Double = Double.PositiveInfinity	// Naive Bayes model
)
