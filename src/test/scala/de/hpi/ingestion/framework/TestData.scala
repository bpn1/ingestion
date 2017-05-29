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

	def configWithouSettingsXML: Node = {
		XML.load(getClass.getResource("/test2.xml"))
	}
}
