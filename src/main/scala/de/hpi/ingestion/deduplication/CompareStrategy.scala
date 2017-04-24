package de.hpi.ingestion.deduplication

import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure


object CompareStrategy extends Serializable {
	/*val strategies = Map(
		"name" -> (
			(attr1: String,
			attr2: String,
			simMeasure: SimilarityMeasure) => Double),
		"aliases" -> (
			(attr1: List[String],
			attr2: List[String],
			simMeasure: SimilarityMeasure) => Double),
		"category" -> (
			(attr1: String,
			attr2: String,
			simMeasure: SimilarityMeasure) => Double),
		"properties" -> (
			(attr1: Map[String, List[String]],
			attr2: Map[String, List[String]],
			simMeasure: SimilarityMeasure) => Double),
		"relations" -> (
			(attr1: Map[UUID, Map[String, String]],
			attr2: Map[UUID, Map[String, String]],
			simMeasure: SimilarityMeasure) => Double),
		"name_history" -> (
			(attr1: List[Version],
			attr2: List[Version],
			simMeasure: SimilarityMeasure) => Double),
		"aliases_history" -> (
			(attr1: List[Version],
			attr2: List[Version],
			simMeasure: SimilarityMeasure) => Double),
		"category_history" -> (
			(attr1: List[Version],
			attr2: List[Version],
			simMeasure: SimilarityMeasure) => Double),
		"properties_history" -> (
			(attr1: Map[String, List[Version]],
			attr2: Map[String, List[Version]],
			simMeasure: SimilarityMeasure) => Double),
		"relations_history" -> (
			(attr1: Map[UUID, Map[String, List[Version]]],
			attr2: Map[UUID, Map[String, List[Version]]],
			simMeasure: SimilarityMeasure) => Double)
	)*/

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

	private def stringListCompare(
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

	private def defaultCompare(
		leftAttributes: List[String],
		rightAttributes: List[String],
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {
		if (leftAttributes.size == 1 && rightAttributes.size == 1) {
			compare(leftAttributes.head, rightAttributes.head, simMeasure, weight, scale)
		} else {
			stringListCompare(leftAttributes, rightAttributes, simMeasure, weight, scale)
		}
	}

	def apply(attribute: String): (List[String], List[String], SimilarityMeasure[String], Double, Int) => Double = {
		attribute match {
			case "name" | "category" | "city" => singleStringCompare
			case "geolocation" => coordinatesCompare
			case "aliases" => stringListCompare
			case default => defaultCompare
		}

	}
}
