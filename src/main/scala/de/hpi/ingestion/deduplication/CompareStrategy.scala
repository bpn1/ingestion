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

	def stringCompare(
		attribute1: List[String],
		attribute2: List[String],
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {
		if(attribute1.nonEmpty && attribute2.nonEmpty) {
			val score = simMeasure.compare(attribute1.head, attribute2.head, scale)
			score * weight
		} else {
			-1.0
		}
	}

	def tupleCompare(
		attribute1: List[String],
		attribute2: List[String],
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {

		1.0
	}

	def stringListCompare(
		attribute1: List[String],
		attribute2: List[String],
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {

		1.0
	}

	def defaultCompare(
		attribute1: List[String],
		attribute2: List[String],
		simMeasure: SimilarityMeasure[String],
		weight: Double,
		scale: Int = 1
	): Double = {

		1.0
	}

	def apply(attribute: String): (List[String], List[String], SimilarityMeasure[String], Double, Int) => Double = {
		attribute match {
			case "name" | "category" | "city" => stringCompare
			case "geolocation" => tupleCompare
			case "aliases" => stringListCompare
			case default => defaultCompare
		}

	}
}
