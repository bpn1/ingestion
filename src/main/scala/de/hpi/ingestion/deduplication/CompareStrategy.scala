package de.hpi.ingestion.deduplication

import de.hpi.ingestion.deduplication.models.config.SimilarityMeasureConfig
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
		scoreConfig: SimilarityMeasureConfig[String, SimilarityMeasure[String]]
	): Double = {
		scoreConfig.compare(leftValue.head, rightValue.head)
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
		scoreConfig: SimilarityMeasureConfig[String, SimilarityMeasure[String]]
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
	): (List[String], List[String], SimilarityMeasureConfig[String, SimilarityMeasure[String]]) => Double = {
		attribute match {
			case "name"
				 | "category"
				 | "geo_city"
				 | "geo_postal"
				 | "geo_country"
				 | "gen_employees" => singleStringCompare
			case "geo_coords"
				 | _ => defaultCompare
		}
	}
}
