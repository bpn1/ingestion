package de.hpi.ingestion.deduplication

import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure

/**
  * This class is a factory of comparing methods for different kinds of values.
  */
object CompareStrategy extends Serializable {
	/**
	  * This method simply compares two strings.
	  * @param leftValue String to be compared with rightValue.
	  * @param rightValue String to be compared with leftValue.
	  * @param simMeasure Similarity measure to be used; as specified when CompareStrategy.apply is called.
	  * @param weight Weight of the feature to be used; as specified when CompareStrategy.apply is called.
	  * @param scale Scale of the feature to be used; as specified when CompareStrategy.apply is called.
	  * @return The similarity score of the two input strings.
	  */
	def compare(
		leftValue: String,
		rightValue: String,
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {
		val score = simMeasure.compare(leftValue, rightValue, scale)
		score * weight
	}

	/**
	  * This method compares two string-lists with one element each.
	  * @param leftValue One-element string-list to be compared with rightValue.
	  * @param rightValue One-element string-list to be compared with leftValue.
	  * @param simMeasure Similarity measure to be used; as specified when CompareStrategy.apply is called.
	  * @param weight Weight of the feature to be used; as specified when CompareStrategy.apply is called.
	  * @param scale Scale of the feature to be used; as specified when CompareStrategy.apply is called.
	  * @return The similarity score of the two input strings.
	  */
	def singleStringCompare(
		leftValue: List[String],
		rightValue: List[String],
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {
		this.compare(leftValue.head, rightValue.head, simMeasure, weight, scale)
	}

	/**
	  * This method naively compares two coordinate-strings.
	  * @param leftValues Coordinates to be compared with rightValue.
	  * @param rightValues Coordinates to be compared with leftValue.
	  * @param simMeasure Similarity measure to be used; as specified when CompareStrategy.apply is called.
	  * @param weight Weight of the feature to be used; as specified when CompareStrategy.apply is called.
	  * @param scale Scale of the feature to be used; as specified when CompareStrategy.apply is called.
	  * @return The similarity score of the two input coordinates.
	  */
	def coordinatesCompare(
		leftValues: List[String],
		rightValues: List[String],
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {
		compare(leftValues.mkString(","), rightValues.mkString(","), simMeasure, weight, scale)
	}

	/**
	  * This method compares two string-lists by default.
	  * @param leftValues List of strings to be compared with rightValues.
	  * @param rightValues List of strings to be compared with leftValues.
	  * @param simMeasure Similarity measure to be used; as specified when CompareStrategy.apply is called.
	  * @param weight Weight of the feature to be used; as specified when CompareStrategy.apply is called.
	  * @param scale Scale of the feature to be used; as specified when CompareStrategy.apply is called.
	  * @return The similarity score of the two input lists of strings.
	  */
	def defaultCompare(
		leftValues: List[String],
		rightValues: List[String],
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {
		val scoreSum = leftValues.map(
			leftValue => rightValues.map(rightValue => compare(leftValue, rightValue, simMeasure, weight, scale))
				.reduceLeft(math.max)).sum
		scoreSum / leftValues.size
	}

	/**
	  * Factory to find the fitting function to compare the given attribute.
	  * @param attribute String defining the method to apply.
	  * @return Function to use for comparison.
	  */
	def apply(attribute: String): (List[String], List[String], SimilarityMeasure[String], Double, Int) => Double = {
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
