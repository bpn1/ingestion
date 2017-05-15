package de.hpi.ingestion.textmining.models

/**
  * Case class representing a value and its rank if compared to certain other values.
  *
  * @param value the absolute value itself
  * @param rank  the rank of the value, starting with 1
  */
case class RankedOccurrence(value: Int, rank: Int = 1)
