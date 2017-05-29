package de.hpi.ingestion.textmining.models

/**
  * #todo
  *
  * @param value
  * @param rank
  * @param delta_top
  * @param delta_successor
  */
case class MultiFeature(
	value: Double,
	var rank: Int = 1,
	var delta_top: Double = 0.0,
	var delta_successor: Double = 0.0 // An Option[Double] would be better but None cannot be stored in a LabeledPoint.
)
