package de.hpi.ingestion.framework

import de.hpi.ingestion.deduplication.models.ScoreConfig
import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure
import scala.io.Source
import scala.xml.{Node, XML}

/**
  * Configurable trait implements a parsing method for configuring the settings.
  */
trait Configurable {
	var configFile: String = ""
	var settings: Map[String, String] = Map()
	var scoreConfigSettings: Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]] = Map()

	/**
	  * Reads the configuration from an xml file.
	  * @return a list containing settings and some options
	  */
	def parseConfig(): Unit = {
		val xml = XML.loadString(Source
			.fromURL(getClass.getResource(s"/configs/$configFile"))
			.getLines()
			.mkString("\n"))
		settings ++= parseSettings(xml)
		scoreConfigSettings ++= parseSimilarityMeasures(xml)
	}

	/**
	  * Parses the settings in the settings tag with xml tags as keys.
	  * @param node the XML node containing the settings tag as child.
	  * @return Map containing the settings
	  */
	def parseSettings(node: Node): Map[String, String] = {
		val settings = node \ "settings"
		settings.headOption.map { settingsNode =>
			settingsNode.child.collect {
				case node: Node if node.text.trim.nonEmpty => (node.label, node.text)
			}.toMap
		}.getOrElse(Map())
	}

	/**
	  * Parses the similarity measures in the simMeasurements tag.
	  * @param node the XML node containing the simMeasurements tag as child.
	  * @return Map containing attributes with their score configurations
	  */
	def parseSimilarityMeasures(node: Node): Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]] = {
		val attributes = node \ "simMeasurements" \ "attribute"
		attributes.map { node =>
			val key = (node \ "key").text
			val scoreConfigs = (node \ "feature").map { feature =>
				val similarityMeasure = (feature \ "similarityMeasure").text
				val weight = (feature \ "weight").text.toDouble
				val scale = (feature \ "scale").text.toInt
				ScoreConfig[String, SimilarityMeasure[String]](
					SimilarityMeasure.get[String](similarityMeasure),
					weight,
					scale)
			}
			(key, scoreConfigs.toList)
		}.toMap
	}
}
