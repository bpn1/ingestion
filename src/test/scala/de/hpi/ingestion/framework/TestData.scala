package de.hpi.ingestion.framework

import de.hpi.ingestion.deduplication.models.ScoreConfig
import de.hpi.ingestion.deduplication.similarity.{JaroWinkler, MongeElkan, SimilarityMeasure}

import scala.xml.{Node, XML}

object TestData {

	def parsedScoreConfig: Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]] = {
		Map(
			"name" -> List(
				ScoreConfig(similarityMeasure = MongeElkan, weight = 0.8),
				ScoreConfig(similarityMeasure = JaroWinkler, weight = 0.7)))
	}

	def parsedSettings: Map[String, String] = {
		Map(
			"key1" -> "val 1",
			"key2" -> "val 2",
			"key3" -> "val 3",
			"key4" -> "val 4")
	}

	def configXML: Node = {
		XML.load(getClass.getResource("/test.xml"))
	}

	def configWithoutSettingsXML: Node = {
		XML.load(getClass.getResource("/test2.xml"))
	}

	def importConfigXML: Node = {
		XML.load(getClass.getResource("/datalake/normalization.xml"))
	}

	def normalizationSettings: Map[String, List[String]] = {
		Map(
			"rootKey" -> List("root_value"),
			"nestedKey1" -> List("nested_value:1.1", "nested_value:1.2", "nested_value:1.3"),
			"nestedKey2" -> List("nested_value:2.1")
		)
	}

	def sectorSettings: Map[String, List[String]] = {
		Map(
			"Category 1" -> List("value1.1", "value1.2"),
			"Category 2" -> List("value2.1"),
			"Category 3" -> List("value3.1", "value3.2")
		)
	}
}
