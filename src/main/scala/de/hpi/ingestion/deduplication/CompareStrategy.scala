package de.hpi.ingestion.deduplication

import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure


object CompareStrategy extends Serializable {
	private def compare(
		leftAttribute: String,
		rightAttribute: String,
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {
		val score = simMeasure.compare(leftAttribute, rightAttribute, scale)
		score * weight
	}

	private def singleStringCompare(
		leftAttribute: List[String],
		rightAttribute: List[String],
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {
		compare(leftAttribute.head, rightAttribute.head, simMeasure, weight, scale)
	}

	private def coordinatesCompare(
		leftAttributes: List[String],
		rightAttributes: List[String],
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {
		val leftX = leftAttributes.head
		val rightX = rightAttributes.head
		val leftY = leftAttributes.last
		val rightY = rightAttributes.last

		val firstScore = compare(leftX, rightX, simMeasure, weight, scale)
		val secondScore = compare(leftY, rightY, simMeasure, weight, scale)
		(firstScore + secondScore) / 2
	}

	private def defaultCompare(
		leftAttributes: List[String],
		rightAttributes: List[String],
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {
		val scoreSum = leftAttributes.map(
			leftAttribute => rightAttributes.map(
				rightAttribute => compare(leftAttribute, rightAttribute, simMeasure, weight, scale)
			).sum
		).sum
		scoreSum / (leftAttributes.size * rightAttributes.size)
	}

	def apply(attribute: String): (List[String], List[String], SimilarityMeasure[String], Double, Int) => Double = {
		attribute match {
			case "name" | "category" | "city" => singleStringCompare
			case "geolocation" => coordinatesCompare
			case default => defaultCompare
		}

	}
}
