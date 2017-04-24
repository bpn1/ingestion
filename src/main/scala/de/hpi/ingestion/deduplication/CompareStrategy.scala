package de.hpi.ingestion.deduplication

import java.util.UUID

import de.hpi.ingestion.datalake.models.Version
import utils.SimilarityMeasure


object CompareStrategy extends Serializable {
	val strategies = Map(
		"name" -> ((String, String, SimilarityMeasure) -> Double),
		"aliases" -> ((List[String], List[String]) -> Double),
		"category" -> ((String, String, SimilarityMeasure) -> Double),
		"properties" -> ((Map[String, List[String]], Map[String, List[String]]) -> Double),
		"relations" -> ((Map[UUID, Map[String, String]], Map[UUID, Map[String, String]]) -> Double),
		"name_history" -> ((List[Version], List[Version]) -> Double),
		"aliases_history" -> ((List[Version], List[Version]) -> Double),
		"category_history" -> ((List[Version], List[Version]) -> Double),
		"properties_history" -> ((Map[String, List[Version]], Map[String, List[Version]]) -> Double),
		"relations_history" ->
			((Map[UUID, Map[String, List[Version]]], Map[UUID, Map[String, List[Version]]])-> Double)
	)
}
