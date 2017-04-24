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
		attribute1: String,
		attribute2: String,
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {
		val score = simMeasure.compare(attribute1, attribute2, scale)
		score * weight
	}

	private def singleStringCompare(
		attribute1: List[String],
		attribute2: List[String],
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {
		compare(attribute1.head, attribute2.head, simMeasure, weight, scale)
	}

	private def tupleCompare(
		attribute1: List[String],
		attribute2: List[String],
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {

		1.0
	}

	private def stringListCompare(
		attributes1: List[String],
		attributes2: List[String],
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {
		val scoreSum = attributes1.map(
			attribute1 => attributes2.map(
				attribute2 => compare(attribute1, attribute2, simMeasure, weight, scale)
			).sum
		).sum
		scoreSum / (attributes1.size * attributes2.size)
	}

	private def defaultCompare(
		attributes1: List[String],
		attributes2: List[String],
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {
		if (attributes1.size == 1 && attributes2.size == 1) {
			compare(attributes1.head, attributes2.head, simMeasure, weight, scale)
		} else {
			stringListCompare(attributes1, attributes2, simMeasure, weight, scale)
		}
	}

	def apply(attribute: String): (List[String], List[String], SimilarityMeasure[String], Double, Int) => Double = {
		attribute match {
			case "name" | "category" | "city" => singleStringCompare
			case "geolocation" => tupleCompare
			case "aliases" => stringListCompare
			case default => defaultCompare
		}

	}
}
