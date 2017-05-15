package de.hpi.ingestion.deduplication

import de.hpi.ingestion.deduplication.models.ScoreConfig
import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure

/**
  * This class is a factory of comparing methods for different kinds of values.
  */
object CompareStrategy extends Serializable {

	/**
	  * This method compares two string-lists with one element each.
	  * @param leftValue One-element string-list to be compared with rightValue.
	  * @param rightValue One-element string-list to be compared with leftValue.
	  * @return The similarity score of the two input strings.
	  */
	def singleStringCompare(
		leftValue: List[String],
		rightValue: List[String],
		scoreConfig: ScoreConfig[String, SimilarityMeasure[String]]
	): Double = {
		scoreConfig.compare(leftValue.head, rightValue.head)
	}

	/**
	  * This method compares two lists of coordinate strings with the appearance ("lat1", "long1", "lat2", "long2"...)
	  * and does so by calculating the average of the maximum scores for each coordinate-pairs.
	  * @param leftValues Coordinates to be compared with rightValue.
	  * @param rightValues Coordinates to be compared with leftValue.
	  * @return The average score of all part-scores.
	  */
	def coordinatesCompare(
		leftValues: List[String],
		rightValues: List[String],
		scoreConfig: ScoreConfig[String, SimilarityMeasure[String]]
	): Double = {
		val left = leftValues.grouped(2).toList.map(_.mkString(","))
		val right = rightValues.grouped(2).toList.map(_.mkString(","))
		val scoreSum = left
			.map { leftValue =>
				right
					.map(rightValue => scoreConfig.compare(leftValue, rightValue))
					.max
			}.sum
		scoreSum / left.size
	}

	/**
	  * This method compares two string-lists by default.
	  * @param leftValues List of strings to be compared with rightValues.
	  * @param rightValues List of strings to be compared with leftValues.
	  * @return The similarity score of the two input lists of strings.
	  */
	def defaultCompare(
		leftValues: List[String],
		rightValues: List[String],
		scoreConfig: ScoreConfig[String, SimilarityMeasure[String]]
	): Double = {
		val scoreSum = leftValues
			.map { leftValue =>
				rightValues
					.map(rightValue => scoreConfig.compare(leftValue, rightValue))
					.max
			}.sum
		scoreSum / leftValues.size
	}

	/**
	  * Factory to find the fitting function to compare the given attribute.
	  * @param attribute String defining the method to apply.
	  * @return Function to use for comparison.
	  */
	def apply(
		attribute: String
	): (List[String], List[String], ScoreConfig[String, SimilarityMeasure[String]]) => Double = {
		attribute match {
			case "name"
				 | "category"
				 | "geo_city"
				 | "geo_postal"
				 | "geo_country"
				 | "gen_employees" => singleStringCompare
			case "geo_coords" => coordinatesCompare
			case _ => defaultCompare
		}
	}
}
