package de.hpi.ingestion.framework

import de.hpi.ingestion.deduplication.models.config.{AttributeConfig, SimilarityMeasureConfig}
import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure

import scala.xml.{Node, XML}

/**
  * Configurable trait implements a parsing method for configuring the settings.
  */
trait Configurable {
	var configFile: String = ""
	private var _settings: Map[String, String] = Map()
	private var _scoreConfigSettings: List[AttributeConfig] = Nil

	/**
	  * Reads the configuration from an xml file.
	  *
	  * @return a list containing settings and some options
	  */
	def parseConfig(): Unit = {
		val xml = XML.load(getClass.getResource(s"/configs/$configFile"))
		_settings ++= parseSettings(xml)
		_scoreConfigSettings ++= parseSimilarityMeasures(xml)
	}

	/**
	  * Getter for _settings. Sets _settings if not yet done.
	  *
	  * @param loadIfEmpty load configuration if not yet done (true by default)
	  * @return _settings
	  */
	def settings(loadIfEmpty: Boolean = true): Map[String, String] = {
		if(_settings.isEmpty && !configFile.isEmpty && loadIfEmpty) {
			parseConfig()
		}
		_settings
	}

	/**
	  * Getter for _settings. Sets _settings if not yet done.
	  *
	  * @return _settings
	  */
	def settings: Map[String, String] = {
		settings()
	}

	/**
	  * Setter for _settings.
	  *
	  * @param newSettings new settings
	  */
	def settings_=(newSettings: Map[String, String]): Unit = _settings = newSettings

	/**
	  * Getter for _scoreConfigSettings. Sets _scoreConfigSettings if not yet done.
	  *
	  * @param loadIfEmpty load configuration if not yet done (true by default)
	  * @return _scoreConfigSettings
	  */
	def scoreConfigSettings(loadIfEmpty: Boolean = true): List[AttributeConfig] = {
		if(_scoreConfigSettings.isEmpty && !configFile.isEmpty && loadIfEmpty) {
			parseConfig()
		}
		_scoreConfigSettings
	}

	/**
	  * Getter for _scoreConfigSettings. Sets _scoreConfigSettings if not yet done.
	  *
	  * @return _scoreConfigSettings
	  */
	def scoreConfigSettings: List[AttributeConfig] = {
		scoreConfigSettings()
	}

	/**
	  * Setter for _scoreConfigSettings.
	  *
	  * @param newSettings new score config settings
	  */
	def scoreConfigSettings_=(newSettings: List[AttributeConfig]): Unit  = {
		_scoreConfigSettings = newSettings
	}

	/**
	  * Parses the settings in the settings tag with xml tags as keys.
	  *
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
	  *
	  * @param node the XML node containing the simMeasurements tag as child.
	  * @return Map containing attributes with their score configurations
	  */
	def parseSimilarityMeasures(node: Node): List[AttributeConfig] = {
		val extractWeight: Node => Double = node => if(node.text.nonEmpty) node.text.toDouble else 0.0
		val attributes = node \ "simMeasurements" \ "attribute"
		val configs = attributes.map { node =>
			val key = (node \ "key").text
			val weight = extractWeight((node \ "weight").head)
			val scoreConfigs = (node \ "feature").map { feature =>
				val similarityMeasure = (feature \ "similarityMeasure").text
				val weight = extractWeight((feature \ "weight").head)
				val scale = (feature \ "scale").text.toInt
				SimilarityMeasureConfig[String, SimilarityMeasure[String]](
					SimilarityMeasure.get[String](similarityMeasure),
					weight,
					scale)
			}.toList
			AttributeConfig(key, weight, scoreConfigs)
		}.toList
		AttributeConfig.normalizeWeights(configs)
	}
}
