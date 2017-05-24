package de.hpi.ingestion.textmining.models

/**
  * #todo
  *
  * @param value
  * @param rank
  * @param deltaTop
  * @param deltaSuccessor
  */
case class MultiFeature(
	value: Double,
	rank: Int = 1,
	deltaTop: Double = 0.0,
	deltaSuccessor: Double = 0.0 // An Option[Double] would be better but None cannot be stored in a LabeledPoint.
)
