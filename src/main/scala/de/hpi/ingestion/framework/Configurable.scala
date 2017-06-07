package de.hpi.ingestion.framework

import de.hpi.ingestion.deduplication.models.ScoreConfig
import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure
import scala.xml.{Node, XML}

/**
  * Configurable trait implements a parsing method for configuring the settings.
  */
trait Configurable {
	var configFile: String = ""
	var importConfigFile: String = ""
	private var _settings: Map[String, String] = Map()
	private var _scoreConfigSettings: Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]] = Map()
	private var _normalizationSettings: Map[String, List[String]] = Map()
	private var _sectorSettings: Map[String, List[String]] = Map()

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

	def parseImportConfig(): Unit = {
		val importXml = if(importConfigFile.contains("/")) {
			XML.loadFile(importConfigFile)
		} else {
			XML.load(getClass.getResource(s"/configs/$importConfigFile"))
		}
		_normalizationSettings = parseNormalizationConfig(importXml)
		_sectorSettings = parseSectorConfig(importXml)
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
	def scoreConfigSettings(
		loadIfEmpty: Boolean = true
	): Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]] = {
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
	def scoreConfigSettings: Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]] = {
		scoreConfigSettings()
	}

	/**
	  * Setter for _scoreConfigSettings.
	  *
	  * @param newSettings new score config settings
	  */
	def scoreConfigSettings_=(newSettings: Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]]): Unit  = {
		_scoreConfigSettings = newSettings
	}

	/**
	  * Getter for _normalizationSettings. Sets _normalizationSettings if not yet done.
	  *
	  * @param loadIfEmpty load configuration if not yet done (true by default)
	  * @return _normalizationSettings
	  */
	def normalizationSettings(loadIfEmpty: Boolean = true): Map[String, List[String]] = {
		if(_normalizationSettings.isEmpty && !importConfigFile.isEmpty && loadIfEmpty) {
			parseImportConfig()
		}
		_normalizationSettings
	}

	/**
	  * Getter for _normalizationSettings. Sets _normalizationSettings if not yet done.
	  *
	  * @return _normalizationSettings
	  */
	def normalizationSettings: Map[String, List[String]] = {
		normalizationSettings()
	}

	/**
	  * Setter for _normalizationSettings.
	  *
	  * @param newSettings new settings
	  */
	def normalizationSettings_=(newSettings: Map[String, List[String]]): Unit = _normalizationSettings = newSettings

	/**
	  * Getter for _sectorSettings. Sets _sectorSettings if not yet done.
	  *
	  * @param loadIfEmpty load configuration if not yet done (true by default)
	  * @return _sectorSettings
	  */
	def sectorSettings(loadIfEmpty: Boolean = true): Map[String, List[String]] = {
		if(_sectorSettings.isEmpty && !importConfigFile.isEmpty && loadIfEmpty) {
			parseImportConfig()
		}
		_sectorSettings
	}

	/**
	  * Getter for _sectorSettings. Sets _sectorSettings if not yet done.
	  *
	  * @return _sectorSettings
	  */
	def sectorSettings: Map[String, List[String]] = {
		sectorSettings()
	}

	/**
	  * Setter for _sectorSettings.
	  *
	  * @param newSettings new settings
	  */
	def sectorSettings_=(newSettings: Map[String, List[String]]): Unit = _sectorSettings = newSettings

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

	/**
	  * Parses the normalization config in the normalization tag in the {@importConfigFile}.
	  * @param node the XML node containing the normalization tag as child.
	  * @return Map containing normalized attribute names with the corresponding source attributes
	  */
	def parseNormalizationConfig(node: Node): Map[String, List[String]] = {
		(node \ "normalization" \ "attributeMapping" \ "attribute").map { attribute =>
			val key = (attribute \ "key").text
			val values = (attribute \ "mapping").map(_.text).toList
			(key, values)
		}.toMap
	}

	/**
	  * Parses the sector config in the categorization tag in the {@importConfigFile}.
	  * @param node the XML node containing the categorization tag as child.
	  * @return Map containing sector names with the corresponding normalized sector names
	  */
	def parseSectorConfig(node: Node): Map[String, List[String]] = {
		(node \ "categorization" \ "category").map { attribute =>
			val key = (attribute \ "key").text
			val values = (attribute \ "mapping").map(_.text).toList
			(key, values)
		}.toMap
	}
}
